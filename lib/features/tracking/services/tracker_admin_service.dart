import 'package:last/core/constants/tracking_config.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/error_handler.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/core/utils/token_hash.dart';
import 'package:last/features/tracking/models/geofence.dart';
import 'package:last/features/tracking/models/gps_tracker.dart';
import 'package:last/features/tracking/models/location_record.dart';
import 'package:last/features/tracking/services/tracking_audit_service.dart';
import 'package:uuid/uuid.dart';

class CreateTrackerResult {
  const CreateTrackerResult({required this.tracker, required this.deviceToken});

  final GpsTracker tracker;
  final String deviceToken;
}

abstract class TrackerAdminService {
  Stream<List<GpsTracker>> watchAllTrackers();
  Future<List<GpsTracker>> getAllTrackers();
  Future<GpsTracker?> getTrackerById(String trackerId);
  Future<CreateTrackerResult> createTracker({required String trackerId, required String deviceIdentifier});
  Future<void> assignTracker({
    required String trackerId,
    required String paroleeId,
    required String paroleeName,
    String? assignedBy,
  });
  Future<void> unassignTracker(String trackerId, {String? actorId});
  Future<void> updateTrackerStatus(String trackerId, TrackerDeviceStatus status);
  Future<void> setIngestAuthUid(String trackerId, String ingestAuthUid);
  Future<String> rotateDeviceToken(String trackerId);
  Future<List<LocationRecord>> getDeviceLocationLogs(String trackerId, {int limit = 50});
  Future<void> assignGeofence({
    required String paroleeId,
    required String name,
    required double centerLat,
    required double centerLng,
    double radiusMeters = TrackingConfig.defaultGeofenceRadiusMeters,
  });
  Future<List<Geofence>> getGeofencesForParolee(String paroleeId);
}

class SupabaseTrackerAdminService implements TrackerAdminService {
  SupabaseTrackerAdminService({TrackingAuditService? auditService})
      : _audit = auditService ?? TrackingAuditService();

  final TrackingAuditService _audit;

  @override
  Stream<List<GpsTracker>> watchAllTrackers() {
    if (!SupabaseConfig.isConfigured) return Stream.value([]);
    try {
      return SupabaseConfig.client
          .from('gps_trackers')
          .stream(primaryKey: ['id'])
          .map((rows) {
            final list = rows.map(_mapTracker).toList()
              ..sort((a, b) => a.id.compareTo(b.id));
            return list;
          })
          .handleError((Object e) {
            AppLogger.e('TrackerAdminService', 'watchAllTrackers note', e);
          });
    } catch (e) {
      AppLogger.e('TrackerAdminService', 'watchAllTrackers setup note', e);
      return Stream.value([]);
    }
  }

  @override
  Future<List<GpsTracker>> getAllTrackers() async {
    if (!SupabaseConfig.isConfigured) return [];
    try {
      final rows = await SupabaseConfig.client.from('gps_trackers').select();
      return (rows as List)
          .map((d) => _mapTracker(Map<String, dynamic>.from(d as Map)))
          .toList();
    } catch (e) {
      AppLogger.e('TrackerAdminService', 'getAllTrackers note', e);
      return [];
    }
  }

  @override
  Future<GpsTracker?> getTrackerById(String trackerId) async {
    if (!SupabaseConfig.isConfigured) return null;
    try {
      final rows = await SupabaseConfig.client
          .from('gps_trackers')
          .select()
          .eq('id', trackerId)
          .limit(1);
      if ((rows as List).isEmpty) return null;
      return _mapTracker(Map<String, dynamic>.from(rows.first as Map));
    } catch (e) {
      AppLogger.e('TrackerAdminService', 'getTrackerById note', e);
      return null;
    }
  }

  @override
  Future<CreateTrackerResult> createTracker({
    required String trackerId,
    required String deviceIdentifier,
  }) async {
    final now = DateTime.now();
    final deviceToken = const Uuid().v4();
    final tracker = GpsTracker(
      id: trackerId,
      deviceIdentifier: deviceIdentifier,
      status: TrackerDeviceStatus.unassigned,
      createdAt: now,
      updatedAt: now,
    );

    try {
      await SupabaseConfig.client.from('gps_trackers').upsert({
        'id': trackerId,
        'device_identifier': deviceIdentifier,
        'device_id': deviceIdentifier,
        'status': TrackerDeviceStatus.unassigned.name,
        'token_hash': sha256Hex(deviceToken),
        'created_at': now.toIso8601String(),
        'updated_at': now.toIso8601String(),
      });
    } catch (_) {
      await SupabaseConfig.client.from('gps_trackers').upsert({
        'id': trackerId,
        'device_id': deviceIdentifier,
        'status': TrackerDeviceStatus.unassigned.name,
        'created_at': now.toIso8601String(),
        'updated_at': now.toIso8601String(),
      });
    }

    await _audit.logDeviceAssignment(
      action: 'device_registered',
      trackerId: trackerId,
      actorRole: 'officer',
    );

    return CreateTrackerResult(tracker: tracker, deviceToken: deviceToken);
  }

  @override
  Future<void> assignTracker({
    required String trackerId,
    required String paroleeId,
    required String paroleeName,
    String? assignedBy,
  }) async {
    try {
      final now = DateTime.now().toIso8601String();

      // Unassign any previous trackers held by this parolee.
      final existing = await SupabaseConfig.client
          .from('gps_trackers')
          .select()
          .eq('parolee_id', paroleeId);
      for (final row in existing as List) {
        final m = Map<String, dynamic>.from(row as Map);
        final existingId = m['id']?.toString() ?? '';
        if (existingId.isNotEmpty && existingId != trackerId) {
          await SupabaseConfig.client.from('gps_trackers').update({
            'parolee_id': null,
            'parolee_name': null,
            'status': TrackerDeviceStatus.unassigned.name,
            'updated_at': now,
          }).eq('id', existingId);
        }
      }

      // Clear previous parolee if tracker was assigned elsewhere.
      final current = await getTrackerById(trackerId);
      if (current?.paroleeId != null && current!.paroleeId != paroleeId) {
        try {
          await SupabaseConfig.client.from('profiles').update({
            'tracker_id': null,
          }).eq('id', current.paroleeId!);
        } catch (_) {}
      }

      await SupabaseConfig.client.from('gps_trackers').update({
        'parolee_id': paroleeId,
        'parolee_name': paroleeName,
        'status': TrackerDeviceStatus.active.name,
        'assigned_at': now,
        'updated_at': now,
      }).eq('id', trackerId);

      try {
        await SupabaseConfig.client.from('profiles').update({
          'tracker_id': trackerId,
        }).eq('id', paroleeId);
      } catch (_) {}

      try {
        await SupabaseConfig.client.from('device_assignments').insert({
          'id': const Uuid().v4(),
          'tracker_id': trackerId,
          'parolee_id': paroleeId,
          'parolee_name': paroleeName,
          'assigned_by': assignedBy,
          'assigned_at': now,
          'status': 'active',
          'created_at': now,
        });
      } catch (_) {}

      await _audit.logDeviceAssignment(
        action: 'device_assigned',
        trackerId: trackerId,
        paroleeId: paroleeId,
        actorId: assignedBy,
        actorRole: 'officer',
      );
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to assign tracker');
    }
  }

  @override
  Future<void> unassignTracker(String trackerId, {String? actorId}) async {
    try {
      final tracker = await getTrackerById(trackerId);
      if (tracker == null) return;
      final now = DateTime.now().toIso8601String();

      await SupabaseConfig.client.from('gps_trackers').update({
        'parolee_id': null,
        'parolee_name': null,
        'status': TrackerDeviceStatus.unassigned.name,
        'updated_at': now,
      }).eq('id', trackerId);

      if (tracker.paroleeId != null && tracker.paroleeId!.isNotEmpty) {
        try {
          await SupabaseConfig.client.from('profiles').update({
            'tracker_id': null,
          }).eq('id', tracker.paroleeId!);
        } catch (_) {}
      }

      await _audit.logDeviceAssignment(
        action: 'device_unassigned',
        trackerId: trackerId,
        paroleeId: tracker.paroleeId,
        actorId: actorId,
        actorRole: 'officer',
      );
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to unassign tracker');
    }
  }

  @override
  Future<void> updateTrackerStatus(String trackerId, TrackerDeviceStatus status) async {
    await SupabaseConfig.client.from('gps_trackers').update({
      'status': status.name,
      'updated_at': DateTime.now().toIso8601String(),
    }).eq('id', trackerId);
  }

  @override
  Future<void> setIngestAuthUid(String trackerId, String ingestAuthUid) async {
    final uid = ingestAuthUid.trim();
    if (uid.isEmpty) {
      throw ErrorHandler.wrap(Exception('empty'), 'Device Auth UID is required');
    }
    try {
      await SupabaseConfig.client.from('gps_trackers').update({
        'ingest_auth_uid': uid,
        'updated_at': DateTime.now().toIso8601String(),
      }).eq('id', trackerId);
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to link device Auth UID');
    }
  }

  @override
  Future<String> rotateDeviceToken(String trackerId) async {
    final newToken = const Uuid().v4();
    await SupabaseConfig.client.from('gps_trackers').update({
      'token_hash': sha256Hex(newToken),
      'updated_at': DateTime.now().toIso8601String(),
    }).eq('id', trackerId);

    await _audit.logDeviceAssignment(
      action: 'device_token_rotated',
      trackerId: trackerId,
      actorRole: 'officer',
    );
    return newToken;
  }

  @override
  Future<List<LocationRecord>> getDeviceLocationLogs(String trackerId, {int limit = 50}) async {
    if (!SupabaseConfig.isConfigured) return [];
    try {
      final rows = await SupabaseConfig.client
          .from('location_logs')
          .select()
          .eq('tracker_id', trackerId)
          .order('created_at', ascending: false)
          .limit(limit);
      return (rows as List).map((d) {
        final r = Map<String, dynamic>.from(d as Map);
        return LocationRecord(
          id: r['id']?.toString() ?? '',
          trackerId: (r['tracker_id'] ?? r['trackerId'])?.toString() ?? trackerId,
          paroleeId: (r['parolee_id'] ?? r['paroleeId'])?.toString() ?? '',
          latitude: (r['latitude'] as num).toDouble(),
          longitude: (r['longitude'] as num).toDouble(),
          accuracy: (r['accuracy'] as num?)?.toDouble() ?? 4.5,
          speed: (r['speed'] as num?)?.toDouble() ?? 0.0,
          batteryLevel: (r['battery_level'] ?? r['batteryLevel']) != null
              ? ((r['battery_level'] ?? r['batteryLevel']) as num).toInt()
              : null,
          recordedAt:
              DateTime.tryParse((r['recorded_at'] ?? r['created_at'])?.toString() ?? '') ??
                  DateTime.now(),
          source: GpsSource.esp32,
        );
      }).toList();
    } catch (e) {
      AppLogger.e('TrackerAdminService', 'getDeviceLocationLogs note', e);
      return [];
    }
  }

  @override
  Future<void> assignGeofence({
    required String paroleeId,
    required String name,
    required double centerLat,
    required double centerLng,
    double radiusMeters = TrackingConfig.defaultGeofenceRadiusMeters,
  }) async {
    final id = const Uuid().v4();
    await SupabaseConfig.client.from('geofences').upsert({
      'id': id,
      'parolee_id': paroleeId,
      'name': name,
      'latitude': centerLat,
      'longitude': centerLng,
      'radius_meters': radiusMeters,
    });
  }

  @override
  Future<List<Geofence>> getGeofencesForParolee(String paroleeId) async {
    if (!SupabaseConfig.isConfigured) return [];
    try {
      final rows = await SupabaseConfig.client
          .from('geofences')
          .select()
          .eq('parolee_id', paroleeId);
      return (rows as List).map((d) {
        final r = Map<String, dynamic>.from(d as Map);
        return Geofence.fromMap({
          'id': r['id']?.toString() ?? '',
          'paroleeId': r['parolee_id'] ?? r['paroleeId'] ?? paroleeId,
          'name': r['name'] ?? 'Monitoring Area',
          'centerLat': r['latitude'] ?? r['center_lat'] ?? r['centerLat'] ?? 0,
          'centerLng': r['longitude'] ?? r['center_lng'] ?? r['centerLng'] ?? 0,
          'radiusMeters': r['radius_meters'] ?? r['radiusMeters'] ?? 500,
          'isActive': r['is_active'] ?? r['isActive'] ?? true,
        });
      }).toList();
    } catch (e) {
      AppLogger.e('TrackerAdminService', 'getGeofencesForParolee note', e);
      return [];
    }
  }

  GpsTracker _mapTracker(Map<String, dynamic> r) {
    return GpsTracker.fromMap({
      'id': r['id']?.toString() ?? '',
      'deviceIdentifier': r['device_identifier'] ?? r['deviceIdentifier'] ?? r['device_id'] ?? '',
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
}

TrackerAdminService createTrackerAdminService() => SupabaseTrackerAdminService();
