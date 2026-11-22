import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/auth/models/app_user.dart';
import 'package:last/features/auth/models/user_role.dart';
import 'package:last/features/tracking/models/geofence.dart';
import 'package:last/features/tracking/models/gps_tracker.dart';
import 'package:last/features/tracking/models/location_record.dart';
import 'package:last/features/tracking/models/parolee_tracking_summary.dart';
import 'package:last/features/tracking/services/check_in_proximity_service.dart';
import 'package:last/features/tracking/services/geofence_evaluation_service.dart';
import 'package:last/features/tracking/services/tracking_authorization_service.dart';
import 'package:last/features/tracking/services/tracking_repository.dart';
import 'package:last/features/tracking/services/tracking_status_service.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class SupabaseTrackingRepository implements TrackingRepository {
  SupabaseTrackingRepository({
    TrackingAuthorizationService? authService,
    TrackingStatusService? statusService,
    GeofenceEvaluationService? geofenceService,
    CheckInProximityService? proximityService,
  })  : _auth = authService ?? TrackingAuthorizationService(),
        _status = statusService ?? TrackingStatusService(),
        _geofence = geofenceService ?? GeofenceEvaluationService(),
        _proximity = proximityService ?? CheckInProximityService();

  final SupabaseClient _supabase = SupabaseConfig.client;
  final TrackingAuthorizationService _auth;
  final TrackingStatusService _status;
  final GeofenceEvaluationService _geofence;
  final CheckInProximityService _proximity;

  final List<StreamController<dynamic>> _controllers = [];

  @override
  Stream<List<ParoleeTrackingSummary>> watchAssignedParolees({
    required AppUser requester,
    required String officerId,
  }) {
    if (requester.role != UserRole.officer) {
      return Stream.error(
        TrackingAuthorizationException('Only officers can view assigned parolees.'),
      );
    }

    final controller = StreamController<List<ParoleeTrackingSummary>>.broadcast();
    _controllers.add(controller);
    final subscriptions = <StreamSubscription<List<Map<String, dynamic>>>>[];
    Timer? debounce;
    Timer? pollTimer;
    var building = false;

    Future<void> emit() async {
      if (controller.isClosed || building) return;
      building = true;
      try {
        final list = await _loadAssignedSummaries(officerId);
        if (!controller.isClosed) {
          controller.add(list);
        }
      } catch (e, st) {
        AppLogger.e('SupabaseTrackingRepo', 'watchAssignedParolees load error', e, st);
        if (!controller.isClosed) {
          controller.addError(e);
        }
      } finally {
        building = false;
      }
    }

    void scheduleEmit() {
      debounce?.cancel();
      debounce = Timer(const Duration(milliseconds: 250), emit);
    }

    void attach(Stream<List<Map<String, dynamic>>> stream) {
      subscriptions.add(
        stream.listen(
          (_) => scheduleEmit(),
          onError: (Object e) {
            AppLogger.e('SupabaseTrackingRepo', 'Realtime stream note', e);
          },
        ),
      );
    }

    try {
      attach(_supabase.from('location_logs').stream(primaryKey: ['id']));
      attach(_supabase.from('gps_trackers').stream(primaryKey: ['id']));
      attach(_supabase.from('geofences').stream(primaryKey: ['id']));
      attach(_supabase.from('profiles').stream(primaryKey: ['id']));
    } catch (e) {
      AppLogger.e('SupabaseTrackingRepo', 'Realtime setup note', e);
    }

    scheduleEmit();
    pollTimer = Timer.periodic(const Duration(seconds: 30), (_) => emit());

    controller.onCancel = () async {
      debounce?.cancel();
      pollTimer?.cancel();
      for (final sub in subscriptions) {
        await sub.cancel();
      }
    };

    return controller.stream;
  }

  @override
  Stream<ParoleeTrackingSummary> watchParoleeTracking({
    required AppUser requester,
    required String paroleeId,
  }) {
    final controller = StreamController<ParoleeTrackingSummary>.broadcast();
    _controllers.add(controller);
    final subscriptions = <StreamSubscription<List<Map<String, dynamic>>>>[];
    Timer? debounce;
    Timer? pollTimer;
    var building = false;

    Future<void> emit() async {
      if (controller.isClosed || building) return;
      building = true;
      try {
        final parolee = await _fetchParoleeUser(paroleeId);
        if (parolee == null) {
          throw TrackingAuthorizationException('Parolee not found.');
        }
        _auth.assertCanTrack(requester, parolee);

        final tracker = await getTrackerForParolee(paroleeId);
        final geofences = await getGeofencesForParolee(paroleeId);
        final latest = await _queryLatestLocation(
          paroleeId: paroleeId,
          trackerId: tracker?.id,
        );

        final summary = _buildSummary(
          parolee: parolee,
          tracker: tracker,
          geofences: geofences,
          latestLocation: latest,
        );
        if (!controller.isClosed) {
          controller.add(summary);
        }
      } catch (e, st) {
        AppLogger.e('SupabaseTrackingRepo', 'watchParoleeTracking error', e, st);
        if (!controller.isClosed) {
          controller.addError(e);
        }
      } finally {
        building = false;
      }
    }

    void scheduleEmit() {
      debounce?.cancel();
      debounce = Timer(const Duration(milliseconds: 250), emit);
    }

    void attach(Stream<List<Map<String, dynamic>>> stream) {
      subscriptions.add(
        stream.listen(
          (_) => scheduleEmit(),
          onError: (Object e) {
            AppLogger.e('SupabaseTrackingRepo', 'Parolee realtime note', e);
          },
        ),
      );
    }

    try {
      attach(_supabase.from('location_logs').stream(primaryKey: ['id']));
      attach(_supabase.from('gps_trackers').stream(primaryKey: ['id']));
      attach(_supabase.from('geofences').stream(primaryKey: ['id']));
      attach(_supabase.from('profiles').stream(primaryKey: ['id']));
    } catch (e) {
      AppLogger.e('SupabaseTrackingRepo', 'Parolee stream setup note', e);
    }

    scheduleEmit();
    pollTimer = Timer.periodic(const Duration(seconds: 20), (_) => emit());

    controller.onCancel = () async {
      debounce?.cancel();
      pollTimer?.cancel();
      for (final sub in subscriptions) {
        await sub.cancel();
      }
    };

    return controller.stream;
  }

  @override
  Future<LocationRecord?> getLatestLocation({
    required AppUser requester,
    required String paroleeId,
  }) async {
    final parolee = await _fetchParoleeUser(paroleeId);
    if (parolee != null) {
      _auth.assertCanTrack(requester, parolee);
    } else if (requester.role != UserRole.officer) {
      throw TrackingAuthorizationException('You are not authorized to track this parolee.');
    }

    final tracker = await getTrackerForParolee(paroleeId);
    return _queryLatestLocation(paroleeId: paroleeId, trackerId: tracker?.id);
  }

  @override
  Future<List<LocationRecord>> getLocationHistory({
    required AppUser requester,
    required String paroleeId,
    DateTime? start,
    DateTime? end,
    int limit = 50,
    String? startAfterId,
  }) async {
    final parolee = await _fetchParoleeUser(paroleeId);
    if (parolee != null) {
      _auth.assertCanTrack(requester, parolee);
    } else if (requester.role != UserRole.officer) {
      throw TrackingAuthorizationException('You are not authorized to track this parolee.');
    }

    final tracker = await getTrackerForParolee(paroleeId);
    try {
      var query = _supabase.from('location_logs').select();
      if (tracker != null && tracker.id.isNotEmpty) {
        query = query.or('parolee_id.eq.$paroleeId,tracker_id.eq.${tracker.id}');
      } else {
        query = query.eq('parolee_id', paroleeId);
      }

      if (start != null) {
        query = query.gte('created_at', start.toUtc().toIso8601String());
      }
      if (end != null) {
        query = query.lte('created_at', end.toUtc().toIso8601String());
      }

      final rows = await query
          .order('created_at', ascending: false)
          .limit(startAfterId != null ? limit * 2 : limit);

      var list = (rows as List)
          .map((r) => _mapLocationRow(
                Map<String, dynamic>.from(r as Map),
                fallbackParoleeId: paroleeId,
              ))
          .toList();

      if (startAfterId != null) {
        final idx = list.indexWhere((r) => r.id == startAfterId);
        if (idx >= 0) {
          list = list.sublist(idx + 1);
        }
      }
      return list.take(limit).toList();
    } catch (e) {
      debugPrint('[Supabase] getLocationHistory note: $e');
      return [];
    }
  }

  @override
  Future<GpsTracker?> getTrackerForParolee(String paroleeId) async {
    try {
      final rows = await _supabase
          .from('gps_trackers')
          .select()
          .eq('parolee_id', paroleeId)
          .limit(1);
      if ((rows as List).isEmpty) return null;
      return _mapTrackerRow(Map<String, dynamic>.from(rows.first as Map));
    } catch (e) {
      debugPrint('[Supabase] getTrackerForParolee note: $e');
      return null;
    }
  }

  @override
  Future<List<Geofence>> getGeofencesForParolee(String paroleeId) async {
    try {
      final rows = await _supabase
          .from('geofences')
          .select()
          .eq('parolee_id', paroleeId);
      return (rows as List)
          .map((d) => _mapGeofenceRow(Map<String, dynamic>.from(d as Map), paroleeId))
          .toList();
    } catch (e) {
      debugPrint('[Supabase] getGeofencesForParolee note: $e');
      return [];
    }
  }

  @override
  Future<void> submitLocation({
    required String trackerId,
    required String deviceToken,
    required LocationRecord location,
  }) async {
    await _supabase.from('location_logs').insert({
      'tracker_id': trackerId,
      'parolee_id': location.paroleeId,
      'latitude': location.latitude,
      'longitude': location.longitude,
      'speed': location.speed,
      'accuracy': location.accuracy,
      'battery_level': location.batteryLevel,
    });
  }

  @override
  void dispose() {
    for (final c in _controllers) {
      if (!c.isClosed) {
        unawaited(c.close());
      }
    }
    _controllers.clear();
  }

  Future<List<ParoleeTrackingSummary>> _loadAssignedSummaries(String officerId) async {
    final profileRows = await _supabase
        .from('profiles')
        .select()
        .eq('role', 'parolee');

    final assignedProfiles = (profileRows as List)
        .map((r) => Map<String, dynamic>.from(r as Map))
        .where((m) {
          final assigned = (m['officer_id'] ?? m['officerId'] ?? m['assignedOfficerId'])?.toString();
          return assigned == officerId;
        })
        .map(_mapProfileUser)
        .toList();

    if (assignedProfiles.isEmpty) return [];

    final trackerRows = await _supabase.from('gps_trackers').select();
    final trackersByParolee = <String, GpsTracker>{};
    for (final r in trackerRows as List) {
      final t = _mapTrackerRow(Map<String, dynamic>.from(r as Map));
      if (t.paroleeId != null && t.paroleeId!.isNotEmpty) {
        trackersByParolee[t.paroleeId!] = t;
      }
    }

    final geofenceRows = await _supabase.from('geofences').select();
    final geofencesByParolee = <String, List<Geofence>>{};
    for (final r in geofenceRows as List) {
      final m = Map<String, dynamic>.from(r as Map);
      final pid = (m['parolee_id'] ?? m['paroleeId'])?.toString() ?? '';
      if (pid.isEmpty) continue;
      geofencesByParolee.putIfAbsent(pid, () => []).add(_mapGeofenceRow(m, pid));
    }

    final recentLogs = await _supabase
        .from('location_logs')
        .select()
        .order('created_at', ascending: false)
        .limit(200);

    final logs = (recentLogs as List)
        .map((r) => Map<String, dynamic>.from(r as Map))
        .toList();

    final summaries = <ParoleeTrackingSummary>[];
    for (final parolee in assignedProfiles) {
      final tracker = trackersByParolee[parolee.id];
      final geofences = geofencesByParolee[parolee.id] ?? const [];

      Map<String, dynamic>? latestRow;
      for (final row in logs) {
        final rowPid = (row['parolee_id'] ?? row['paroleeId'])?.toString();
        final rowTid = (row['tracker_id'] ?? row['trackerId'])?.toString();
        if (rowPid == parolee.id || (tracker != null && rowTid == tracker.id)) {
          latestRow = row;
          break;
        }
      }

      final latestLoc = latestRow != null
          ? _mapLocationRow(latestRow, fallbackParoleeId: parolee.id)
          : await _queryLatestLocation(paroleeId: parolee.id, trackerId: tracker?.id);

      summaries.add(
        _buildSummary(
          parolee: parolee,
          tracker: tracker,
          geofences: geofences,
          latestLocation: latestLoc,
        ),
      );
    }

    summaries.sort((a, b) => a.fullName.compareTo(b.fullName));
    return summaries;
  }

  ParoleeTrackingSummary _buildSummary({
    required AppUser parolee,
    required GpsTracker? tracker,
    required List<Geofence> geofences,
    required LocationRecord? latestLocation,
  }) {
    final geofenceStatus = _geofence.evaluate(
      location: latestLocation,
      geofences: geofences,
    );
    final trackingStatus = _status.resolve(
      tracker: tracker,
      latestLocation: latestLocation,
    );
    final checkInProximity = _proximity.evaluate(
      isAuthorized: tracker != null && tracker.isAssigned,
      tracker: tracker,
      location: latestLocation,
      geofences: geofences,
      isSimulated: false,
    );

    return ParoleeTrackingSummary(
      paroleeId: parolee.id,
      fullName: parolee.fullName,
      caseNumber: parolee.caseNumber ?? 'N/A',
      officerId: parolee.officerId ?? '',
      trackingStatus: trackingStatus,
      tracker: tracker,
      latestLocation: latestLocation,
      geofenceStatus: geofenceStatus,
      geofences: geofences,
      checkInProximity: checkInProximity,
    );
  }

  Future<AppUser?> _fetchParoleeUser(String paroleeId) async {
    try {
      final rows = await _supabase
          .from('profiles')
          .select()
          .eq('id', paroleeId)
          .limit(1);
      if ((rows as List).isEmpty) return null;
      return _mapProfileUser(Map<String, dynamic>.from(rows.first as Map));
    } catch (e) {
      debugPrint('[Supabase] _fetchParoleeUser note: $e');
      return null;
    }
  }

  Future<LocationRecord?> _queryLatestLocation({
    required String paroleeId,
    String? trackerId,
  }) async {
    try {
      var query = _supabase.from('location_logs').select();
      if (trackerId != null && trackerId.isNotEmpty) {
        query = query.or('parolee_id.eq.$paroleeId,tracker_id.eq.$trackerId');
      } else {
        query = query.eq('parolee_id', paroleeId);
      }
      final res = await query.order('created_at', ascending: false).limit(1);
      if ((res as List).isNotEmpty) {
        return _mapLocationRow(
          Map<String, dynamic>.from(res.first as Map),
          fallbackParoleeId: paroleeId,
        );
      }
    } catch (e) {
      debugPrint('[Supabase] getLatestLocation note: $e');
    }
    return null;
  }

  AppUser _mapProfileUser(Map<String, dynamic> m) {
    return AppUser.fromMap({
      'id': m['id']?.toString() ?? '',
      'email': m['email'] ?? '',
      'fullName': m['full_name'] ?? m['fullName'] ?? 'Unknown',
      'role': m['role'] ?? 'parolee',
      'department': m['department'],
      'caseNumber': m['case_number'] ?? m['caseNumber'],
      'officerId': m['officer_id'] ?? m['officerId'] ?? m['assignedOfficerId'],
      'trackerId': m['tracker_id'] ?? m['trackerId'],
      'avatarInitials': m['avatar_initials'] ?? m['avatarInitials'],
      'phone': m['phone'],
      'isActive': m['is_active'] ?? m['isActive'] ?? true,
      'supervisionStartDate': m['supervision_start_date'] ?? m['supervisionStartDate'],
      'supervisionEndDate': m['supervision_end_date'] ?? m['supervisionEndDate'],
      'supervisionIsSuspended': m['supervision_is_suspended'] ?? m['supervisionIsSuspended'] ?? false,
    });
  }

  GpsTracker _mapTrackerRow(Map<String, dynamic> r) {
    return GpsTracker.fromMap({
      'id': r['id']?.toString() ?? '',
      'deviceIdentifier': r['device_identifier'] ?? r['deviceIdentifier'] ?? r['device_id'] ?? r['id'] ?? '',
      'deviceName': r['device_name'] ?? r['deviceName'],
      'deviceType': r['device_type'] ?? r['deviceType'],
      'serialNumber': r['serial_number'] ?? r['serialNumber'],
      'paroleeId': r['parolee_id'] ?? r['paroleeId'],
      'paroleeName': r['parolee_name'] ?? r['paroleeName'],
      'status': r['status'],
      'batteryLevel': r['battery_level'] ?? r['batteryLevel'],
      'lastSeenAt': r['last_seen_at'] ?? r['lastSeenAt'],
      'assignedAt': r['assigned_at'] ?? r['assignedAt'],
      'createdAt': r['created_at'] ?? r['createdAt'],
      'updatedAt': r['updated_at'] ?? r['updatedAt'],
      'ingestAuthUid': r['ingest_auth_uid'] ?? r['ingestAuthUid'],
    });
  }

  Geofence _mapGeofenceRow(Map<String, dynamic> r, String fallbackParoleeId) {
    return Geofence.fromMap({
      'id': r['id']?.toString() ?? '',
      'paroleeId': r['parolee_id'] ?? r['paroleeId'] ?? fallbackParoleeId,
      'name': r['name'] ?? 'Monitoring Area',
      'centerLat': r['center_lat'] ?? r['latitude'] ?? r['centerLat'] ?? 0,
      'centerLng': r['center_lng'] ?? r['longitude'] ?? r['centerLng'] ?? 0,
      'radiusMeters': r['radius_meters'] ?? r['radiusMeters'] ?? 500,
      'isActive': r['is_active'] ?? r['isActive'] ?? true,
    });
  }

  LocationRecord _mapLocationRow(
    Map<String, dynamic> row, {
    required String fallbackParoleeId,
  }) {
    final trackerId = (row['tracker_id'] ?? row['trackerId'])?.toString() ?? 'TRK-001';
    final parsedSource = GpsSource.tryParse(row['source']?.toString());
    final inferredSource = parsedSource ??
        (trackerId == 'MOBILE-GPS' ? GpsSource.mobile : GpsSource.esp32);

    return LocationRecord(
      id: row['id']?.toString() ?? DateTime.now().millisecondsSinceEpoch.toString(),
      trackerId: trackerId,
      paroleeId: (row['parolee_id'] ?? row['paroleeId'])?.toString() ?? fallbackParoleeId,
      latitude: (row['latitude'] as num).toDouble(),
      longitude: (row['longitude'] as num).toDouble(),
      accuracy: (row['accuracy'] as num?)?.toDouble() ?? 4.5,
      speed: (row['speed'] as num?)?.toDouble() ?? 0.0,
      batteryLevel: (row['battery_level'] ?? row['batteryLevel']) != null
          ? ((row['battery_level'] ?? row['batteryLevel']) as num).toInt()
          : null,
      recordedAt: DateTime.tryParse(
            (row['recorded_at'] ?? row['created_at'])?.toString() ?? '',
          )?.toLocal() ??
          DateTime.now(),
      source: inferredSource,
    );
  }
}
