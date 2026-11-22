import 'package:last/core/services/notification_service.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/activities/models/activity_attendance_record.dart';
import 'package:last/features/activities/models/activity_model.dart';
import 'package:last/features/activities/models/activity_qr_payload.dart';
import 'package:last/features/activities/services/activity_qr_service.dart';
import 'package:last/features/auth/models/app_user.dart';
import 'package:last/features/auth/models/user_role.dart';
import 'package:last/features/shared/models/notification_model.dart';
import 'package:uuid/uuid.dart';

class ActivityScanResult {
  const ActivityScanResult.success(this.record) : error = null;
  const ActivityScanResult.failure(this.error) : record = null;

  final ActivityAttendanceRecord? record;
  final String? error;

  bool get isSuccess => record != null;
}

class ActivityAttendanceService {
  ActivityAttendanceService({
    ActivityQrService? qrService,
    NotificationService? notificationService,
  })  : _qrService = qrService ?? ActivityQrService(),
        _notifications = notificationService ?? createNotificationService();

  final ActivityQrService _qrService;
  final NotificationService _notifications;

  Stream<List<ActivityAttendanceRecord>> watchForActivity(String activityId) {
    if (!SupabaseConfig.isConfigured) return Stream.value([]);
    try {
      return SupabaseConfig.client
          .from('activity_attendance')
          .stream(primaryKey: ['id'])
          .eq('activity_id', activityId)
          .map(_mapRows)
          .handleError((Object e) {
            AppLogger.e('ActivityAttendanceService', 'watchForActivity note', e);
          });
    } catch (e) {
      AppLogger.e('ActivityAttendanceService', 'watchForActivity setup note', e);
      return Stream.value([]);
    }
  }

  Stream<List<ActivityAttendanceRecord>> watchForParolee(String paroleeId) {
    if (!SupabaseConfig.isConfigured) return Stream.value([]);
    try {
      return SupabaseConfig.client
          .from('activity_attendance')
          .stream(primaryKey: ['id'])
          .eq('parolee_id', paroleeId)
          .map(_mapRows)
          .handleError((Object e) {
            AppLogger.e('ActivityAttendanceService', 'watchForParolee note', e);
          });
    } catch (e) {
      AppLogger.e('ActivityAttendanceService', 'watchForParolee setup note', e);
      return Stream.value([]);
    }
  }

  Stream<List<ActivityAttendanceRecord>> watchAll() {
    if (!SupabaseConfig.isConfigured) return Stream.value([]);
    try {
      return SupabaseConfig.client
          .from('activity_attendance')
          .stream(primaryKey: ['id'])
          .map(_mapRows)
          .handleError((Object e) {
            AppLogger.e('ActivityAttendanceService', 'watchAll note', e);
          });
    } catch (e) {
      AppLogger.e('ActivityAttendanceService', 'watchAll setup note', e);
      return Stream.value([]);
    }
  }

  Stream<ActivityAttendanceStats> watchStatsForActivity(String activityId) {
    return watchForActivity(activityId).map(_computeStats);
  }

  ActivityAttendanceStats _computeStats(List<ActivityAttendanceRecord> list) {
    return ActivityAttendanceStats(
      present: list.where((r) => r.status == ActivityParticipationStatus.present).length,
      absent: list.where((r) => r.status == ActivityParticipationStatus.absent).length,
      late: list.where((r) => r.status == ActivityParticipationStatus.late).length,
      pending: list.where((r) => r.status == ActivityParticipationStatus.pending).length,
      total: list.length,
    );
  }

  Future<ActivityAttendanceRecord?> getForParoleeActivity(
    String activityId,
    String paroleeId,
  ) async {
    if (!SupabaseConfig.isConfigured) return null;
    try {
      final rows = await SupabaseConfig.client
          .from('activity_attendance')
          .select()
          .eq('activity_id', activityId)
          .eq('parolee_id', paroleeId)
          .limit(1);
      if ((rows as List).isEmpty) return null;
      return _mapRow(Map<String, dynamic>.from(rows.first as Map));
    } catch (e) {
      AppLogger.e('ActivityAttendanceService', 'getForParoleeActivity note', e);
      return null;
    }
  }

  Future<ActivityScanResult> scanAndMark({
    required String rawQr,
    required AppUser officer,
    required String expectedActivityId,
    required ActivityParticipationStatus status,
    required ActivityModel activity,
  }) async {
    if (!SupabaseConfig.isConfigured) {
      return const ActivityScanResult.failure('Supabase is not configured');
    }
    if (officer.role != UserRole.officer) {
      return const ActivityScanResult.failure('Only officers can mark activity attendance');
    }

    final decoded = ActivityQrPayload.decode(rawQr);
    if (decoded == null || decoded.token.isEmpty) {
      return const ActivityScanResult.failure('Invalid QR code format');
    }
    var payload = await _qrService.resolveFromScan(
      token: decoded.token,
      activityId: expectedActivityId,
    );
    if (payload == null && decoded.paroleeId.isNotEmpty) {
      final ok = await _qrService.validateToken(decoded);
      if (ok) payload = decoded;
    }
    if (payload == null) {
      return const ActivityScanResult.failure('QR code is invalid or was not issued for this activity.');
    }
    if (payload.activityId != expectedActivityId) {
      return const ActivityScanResult.failure('QR code is for a different activity');
    }
    if (payload.officerId.isNotEmpty && payload.officerId != officer.id) {
      return const ActivityScanResult.failure('This QR is not assigned to you');
    }
    if (!activity.isAssignedTo(payload.paroleeId)) {
      return const ActivityScanResult.failure('Parolee is not assigned to this activity');
    }

    final existing = await getForParoleeActivity(expectedActivityId, payload.paroleeId);
    if (existing != null && existing.status.countsAsPresent && status.countsAsPresent) {
      return ActivityScanResult.failure(
        '${existing.paroleeName} is already marked ${existing.status.label}',
      );
    }

    try {
      final now = DateTime.now();
      final id = existing?.id ?? const Uuid().v4();
      final record = ActivityAttendanceRecord(
        id: id,
        activityId: expectedActivityId,
        paroleeId: payload.paroleeId,
        paroleeName: payload.paroleeName,
        officerId: officer.id,
        status: status,
        scannedAt: now,
        verifiedByOfficer: true,
        timestamp: payload.timestamp,
        activityTitle: activity.title,
      );

      await SupabaseConfig.client.from('activity_attendance').upsert({
        'id': id,
        'activity_id': expectedActivityId,
        'parolee_id': payload.paroleeId,
        'parolee_name': payload.paroleeName,
        'officer_id': officer.id,
        'status': status.name,
        'scanned_at': now.toIso8601String(),
        'verified_by_officer': true,
        'timestamp': payload.timestamp.toIso8601String(),
        'activity_title': activity.title,
        'record_type': 'activity',
      });

      await _syncActivityCompletion(activity, payload.paroleeId, status);

      await _notifications.send(
        userId: payload.paroleeId,
        title: 'Activity Attendance Recorded',
        body: 'Your attendance for ${activity.title} was marked ${status.label} by your officer.',
        type: NotificationType.success,
      );

      return ActivityScanResult.success(record);
    } catch (e) {
      return ActivityScanResult.failure('Failed to save attendance: $e');
    }
  }

  Future<void> _syncActivityCompletion(
    ActivityModel activity,
    String paroleeId,
    ActivityParticipationStatus status,
  ) async {
    final completed = List<String>.from(activity.completedParoleeIds);
    if (status.countsAsPresent) {
      if (!completed.contains(paroleeId)) completed.add(paroleeId);
    } else if (status == ActivityParticipationStatus.absent) {
      completed.remove(paroleeId);
    }

    var activityStatus = activity.status;
    if (completed.length >= activity.assignedParoleeIds.length &&
        activity.assignedParoleeIds.isNotEmpty) {
      activityStatus = ActivityStatus.completed;
    } else if (activity.status == ActivityStatus.pending) {
      activityStatus = ActivityStatus.ongoing;
    }

    await SupabaseConfig.client.from('activities').update({
      'completed_parolees': completed,
      'status': activityStatus.name,
    }).eq('id', activity.id);
  }

  List<ActivityAttendanceRecord> _mapRows(List<Map<String, dynamic>> rows) {
    final list = rows.map(_mapRow).toList()
      ..sort((a, b) => b.scannedAt.compareTo(a.scannedAt));
    return list;
  }

  ActivityAttendanceRecord _mapRow(Map<String, dynamic> r) {
    return ActivityAttendanceRecord.fromMap({
      'id': r['id']?.toString() ?? '',
      'activityId': r['activity_id'] ?? r['activityId'] ?? '',
      'paroleeId': r['parolee_id'] ?? r['paroleeId'] ?? '',
      'paroleeName': r['parolee_name'] ?? r['paroleeName'] ?? '',
      'officerId': r['officer_id'] ?? r['officerId'] ?? '',
      'status': r['status'] ?? 'pending',
      'scannedAt': r['scanned_at'] ?? r['scannedAt'] ?? DateTime.now().toIso8601String(),
      'verifiedByOfficer': r['verified_by_officer'] ?? r['verifiedByOfficer'] ?? true,
      'timestamp': r['timestamp'] ?? DateTime.now().toIso8601String(),
      'activityTitle': r['activity_title'] ?? r['activityTitle'],
    });
  }
}
