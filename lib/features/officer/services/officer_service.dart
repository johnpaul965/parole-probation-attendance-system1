import 'dart:async';

import 'package:last/core/services/live_stats_service.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/date_utils.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/core/utils/validators.dart';
import 'package:last/features/auth/models/app_user.dart';
import 'package:last/features/auth/models/supervision_period.dart';
import 'package:last/features/officer/models/dashboard_stats.dart';
import 'package:last/features/officer/models/parolee_summary.dart';
import 'package:last/features/shared/models/attendance_record.dart';
import 'package:last/features/shared/models/schedule_model.dart';
import 'package:last/features/tracking/services/tracking_audit_service.dart';

class OfficerService {
  OfficerService({
    LiveStatsService? liveStatsService,
    TrackingAuditService? auditService,
  })  : _liveStats = liveStatsService ?? LiveStatsService(),
        _audit = auditService ?? TrackingAuditService();

  final LiveStatsService _liveStats;
  final TrackingAuditService _audit;

  Stream<DashboardSnapshot> watchDashboard(
    String officerId, {
    String? query,
    AttendanceStatus? filter,
  }) {
    if (!SupabaseConfig.isConfigured) {
      AppLogger.d('OfficerService', 'Supabase unavailable — empty dashboard');
      return Stream.value(const DashboardSnapshot(
        stats: DashboardStats(totalCases: 0, checkedIn: 0, pending: 0, missed: 0),
        parolees: [],
        schedules: [],
      ));
    }

    if (officerId.isEmpty) {
      return Stream.error(Exception('Officer ID is missing. Sign out and sign in again.'));
    }

    final controller = StreamController<DashboardSnapshot>();
    final subscriptions = <StreamSubscription<List<Map<String, dynamic>>>>[];
    Timer? debounce;
    var building = false;

    Future<void> emit() async {
      if (controller.isClosed || building) return;
      building = true;
      try {
        final snapshot = await _buildDashboard(
          officerId,
          query: query,
          filter: filter,
        );
        if (!controller.isClosed) {
          controller.add(snapshot);
        }
      } catch (e, st) {
        AppLogger.e('OfficerService', 'Dashboard build failed', e, st);
        if (!controller.isClosed) {
          controller.addError(Exception(e.toString().replaceFirst('Exception: ', '')));
        }
      } finally {
        building = false;
      }
    }

    void scheduleEmit() {
      debounce?.cancel();
      debounce = Timer(const Duration(milliseconds: 250), emit);
    }

    void attachStream(Stream<List<Map<String, dynamic>>> stream) {
      subscriptions.add(
        stream.listen(
          (_) => scheduleEmit(),
          onError: (Object e, StackTrace st) {
            AppLogger.e('OfficerService', 'Supabase listener note', e, st);
          },
        ),
      );
    }

    try {
      attachStream(SupabaseConfig.client.from('profiles').stream(primaryKey: ['id']));
      attachStream(SupabaseConfig.client.from('attendance_records').stream(primaryKey: ['id']));
      attachStream(SupabaseConfig.client.from('schedules').stream(primaryKey: ['id']));
    } catch (e, st) {
      AppLogger.e('OfficerService', 'Stream setup note', e, st);
    }

    scheduleEmit();

    controller.onCancel = () async {
      debounce?.cancel();
      for (final sub in subscriptions) {
        await sub.cancel();
      }
    };

    return controller.stream;
  }

  Future<DashboardSnapshot> _buildDashboard(
    String officerId, {
    String? query,
    AttendanceStatus? filter,
  }) async {
    final parolees = await getParolees(officerId, query: query, filter: filter);
    final schedules = await getSchedules(officerId);

    try {
      await _liveStats.publish(officerId, parolees);
    } catch (e, st) {
      AppLogger.e('OfficerService', 'live_stats publish failed (dashboard still loads)', e, st);
    }

    return DashboardSnapshot(
      stats: DashboardStats(
        totalCases: parolees.length,
        checkedIn: parolees
            .where((p) =>
                p.status == AttendanceStatus.checkedIn ||
                p.status == AttendanceStatus.confirmed)
            .length,
        pending: parolees.where((p) => p.status == AttendanceStatus.pending).length,
        missed: parolees.where((p) => p.status == AttendanceStatus.missed).length,
      ),
      parolees: parolees,
      schedules: schedules,
    );
  }

  Future<List<Map<String, dynamic>>> _fetchParoleeProfiles(String officerId) async {
    try {
      final rows = await SupabaseConfig.client
          .from('profiles')
          .select()
          .eq('role', 'parolee');
      return (rows as List)
          .map((r) => Map<String, dynamic>.from(r as Map))
          .where((m) {
            final assigned = (m['officer_id'] ?? m['officerId'] ?? m['assignedOfficerId'])?.toString();
            return assigned == officerId || officerId == 'demo-officer';
          })
          .toList();
    } catch (e, st) {
      AppLogger.e('OfficerService', '_fetchParoleeProfiles note', e, st);
      return [];
    }
  }

  Future<List<ParoleeSummary>> getParolees(
    String officerId, {
    String? query,
    AttendanceStatus? filter,
  }) async {
    final profileRows = await _fetchParoleeProfiles(officerId);

    final allAttendance = <AttendanceRecord>[];
    try {
      final attRows = await SupabaseConfig.client
          .from('attendance_records')
          .select()
          .eq('officer_id', officerId);
      for (final r in attRows as List) {
        try {
          final data = Map<String, dynamic>.from(r as Map);
          if (data['record_type'] == 'activity' || data['recordType'] == 'activity') continue;
          allAttendance.add(_mapAttendance(data));
        } catch (e) {
          AppLogger.e('OfficerService', 'Skip invalid attendance row', e);
        }
      }
    } catch (e, st) {
      AppLogger.e('OfficerService', 'attendance_records fetch note', e, st);
    }

    final overdueSchedules = <ScheduleModel>[];
    final now = DateTime.now();
    try {
      final schRows = await SupabaseConfig.client
          .from('schedules')
          .select()
          .eq('officer_id', officerId);
      for (final r in schRows as List) {
        try {
          final s = _mapSchedule(Map<String, dynamic>.from(r as Map));
          if (s.scheduledAt.isBefore(now)) overdueSchedules.add(s);
        } catch (e) {
          AppLogger.e('OfficerService', 'Skip invalid schedule row', e);
        }
      }
    } catch (e, st) {
      AppLogger.e('OfficerService', 'schedules fetch note', e, st);
    }

    var list = profileRows.map((data) {
      final paroleeId = data['id']?.toString() ?? '';
      final status = _resolveStatus(paroleeId, allAttendance, overdueSchedules);
      final todayRecord = allAttendance
          .where((a) => a.paroleeId == paroleeId && AppDateUtils.isToday(a.checkInTime))
          .toList()
        ..sort((a, b) =>
            (b.checkInTime ?? DateTime(2000)).compareTo(a.checkInTime ?? DateTime(2000)));

      final supervision = SupervisionPeriod.compute(
        startRaw: data['supervision_start_date'] ?? data['supervisionStartDate'],
        endRaw: data['supervision_end_date'] ?? data['supervisionEndDate'],
        isSuspended: (data['supervision_is_suspended'] ?? data['supervisionIsSuspended']) as bool? ?? false,
      );

      return ParoleeSummary(
        id: paroleeId,
        fullName: (data['full_name'] ?? data['fullName']) as String? ?? 'Unknown',
        caseNumber: (data['case_number'] ?? data['caseNumber']) as String? ?? 'N/A',
        status: status,
        officerId: officerId,
        lastCheckIn: todayRecord.isNotEmpty ? todayRecord.first.checkInTime : null,
        supervisionDatesMissing: supervision.hasMissingDates,
        supervisionShortLabel: supervision.shortLabel,
      );
    }).toList();

    if (query != null && query.isNotEmpty) {
      final q = query.toLowerCase();
      list = list
          .where(
            (p) =>
                p.fullName.toLowerCase().contains(q) ||
                p.caseNumber.toLowerCase().contains(q),
          )
          .toList();
    }
    if (filter != null) {
      list = list.where((p) => p.status == filter).toList();
    }
    return list;
  }

  AttendanceStatus _resolveStatus(
    String paroleeId,
    List<AttendanceRecord> allAttendance,
    List<ScheduleModel> overdueSchedules,
  ) {
    final todayRecords = allAttendance.where(
      (a) => a.paroleeId == paroleeId && AppDateUtils.isToday(a.checkInTime),
    );
    if (todayRecords.any((r) =>
        r.status == AttendanceStatus.checkedIn ||
        r.status == AttendanceStatus.confirmed)) {
      return AttendanceStatus.checkedIn;
    }

    final missedSchedule = overdueSchedules.any((s) {
      if (s.paroleeId != paroleeId) return false;
      final hasCheckIn = allAttendance.any(
        (a) =>
            a.paroleeId == paroleeId &&
            AppDateUtils.isSameDay(a.checkInTime, s.scheduledAt),
      );
      return !hasCheckIn;
    });
    if (missedSchedule) return AttendanceStatus.missed;

    return AttendanceStatus.pending;
  }

  Future<List<ScheduleModel>> getSchedules(String officerId) async {
    try {
      final rows = await SupabaseConfig.client
          .from('schedules')
          .select()
          .eq('officer_id', officerId);
      final list = <ScheduleModel>[];
      for (final r in rows as List) {
        try {
          list.add(_mapSchedule(Map<String, dynamic>.from(r as Map)));
        } catch (e) {
          AppLogger.e('OfficerService', 'Skip invalid schedule', e);
        }
      }
      list.sort((a, b) => a.scheduledAt.compareTo(b.scheduledAt));
      return list.where((s) => s.scheduledAt.isAfter(DateTime.now())).toList();
    } catch (e, st) {
      AppLogger.e('OfficerService', 'getSchedules note', e, st);
      return [];
    }
  }

  Future<AppUser?> getUserById(String id) async {
    try {
      final rows = await SupabaseConfig.client
          .from('profiles')
          .select()
          .eq('id', id)
          .limit(1);
      if ((rows as List).isEmpty) return null;
      return _mapUser(Map<String, dynamic>.from(rows.first as Map));
    } catch (e, st) {
      AppLogger.e('OfficerService', 'getUserById note', e, st);
      return null;
    }
  }

  Future<void> updateSupervisionPeriod({
    required String paroleeId,
    required DateTime start,
    required DateTime end,
    required bool isSuspended,
    required String actorId,
    required String actorRole,
  }) async {
    final rangeError = Validators.supervisionRange(start, end);
    if (rangeError != null) {
      throw Exception(rangeError);
    }

    final existing = await getUserById(paroleeId);
    if (existing == null) {
      throw Exception('Parolee not found.');
    }

    final startIso = SupervisionPeriod.formatIso(start);
    final endIso = SupervisionPeriod.formatIso(end);

    try {
      await SupabaseConfig.client.from('profiles').update({
        'supervision_start_date': startIso,
        'supervision_end_date': endIso,
        'supervision_is_suspended': isSuspended,
      }).eq('id', paroleeId);
    } catch (e) {
      throw Exception('Failed to save supervision dates: $e');
    }

    try {
      await _audit.logSupervisionPeriodUpdated(
        paroleeId: paroleeId,
        actorId: actorId,
        actorRole: actorRole,
        metadata: {
          'previousStart': existing.supervisionStartDate,
          'previousEnd': existing.supervisionEndDate,
          'previousSuspended': existing.supervisionIsSuspended,
          'start': startIso,
          'end': endIso,
          'suspended': isSuspended,
        },
      );
    } catch (e, st) {
      AppLogger.e('OfficerService', 'Supervision audit log failed', e, st);
    }
  }

  AttendanceRecord _mapAttendance(Map<String, dynamic> r) {
    return AttendanceRecord(
      id: r['id']?.toString() ?? '',
      ticketId: (r['ticket_id'] ?? r['ticketId'])?.toString() ?? '',
      paroleeId: (r['parolee_id'] ?? r['paroleeId'])?.toString() ?? '',
      paroleeName: (r['parolee_name'] ?? r['paroleeName'])?.toString() ?? 'Parolee',
      officerId: (r['officer_id'] ?? r['officerId'])?.toString() ?? '',
      status: AttendanceStatus.fromString(r['status']?.toString() ?? 'pending'),
      checkInTime: (r['check_in_time'] ?? r['checkInTime']) != null
          ? DateTime.tryParse((r['check_in_time'] ?? r['checkInTime']).toString())
          : null,
      scheduledTime: (r['scheduled_time'] ?? r['scheduledTime']) != null
          ? DateTime.tryParse((r['scheduled_time'] ?? r['scheduledTime']).toString())
          : null,
      location: r['location']?.toString(),
      verified: r['verified'] == true,
    );
  }

  ScheduleModel _mapSchedule(Map<String, dynamic> r) {
    return ScheduleModel.fromMap({
      'id': r['id']?.toString() ?? '',
      'paroleeId': r['parolee_id'] ?? r['paroleeId'] ?? '',
      'paroleeName': r['parolee_name'] ?? r['paroleeName'] ?? '',
      'officerId': r['officer_id'] ?? r['officerId'] ?? '',
      'scheduledAt': r['scheduled_at'] ?? r['scheduledAt'] ?? DateTime.now().toIso8601String(),
      'location': r['location'] ?? '',
      'type': r['type'] ?? 'Check-in',
      'notes': r['notes'],
    });
  }

  AppUser _mapUser(Map<String, dynamic> m) {
    return AppUser.fromMap({
      'id': m['id']?.toString() ?? '',
      'email': m['email'] ?? '',
      'fullName': m['full_name'] ?? m['fullName'] ?? '',
      'role': m['role'] ?? 'parolee',
      'department': m['department'],
      'caseNumber': m['case_number'] ?? m['caseNumber'],
      'officerId': m['officer_id'] ?? m['officerId'] ?? m['assignedOfficerId'],
      'trackerId': m['tracker_id'] ?? m['trackerId'] ?? m['assignedTrackerId'],
      'avatarInitials': m['avatar_initials'] ?? m['avatarInitials'],
      'phone': m['phone'],
      'isActive': m['is_active'] ?? m['isActive'] ?? true,
      'supervisionStartDate': m['supervision_start_date'] ?? m['supervisionStartDate'],
      'supervisionEndDate': m['supervision_end_date'] ?? m['supervisionEndDate'],
      'supervisionIsSuspended': m['supervision_is_suspended'] ?? m['supervisionIsSuspended'] ?? false,
    });
  }
}

class DashboardSnapshot {
  const DashboardSnapshot({
    required this.stats,
    required this.parolees,
    required this.schedules,
  });

  final DashboardStats stats;
  final List<ParoleeSummary> parolees;
  final List<ScheduleModel> schedules;
}

OfficerService createOfficerService() => OfficerService();
