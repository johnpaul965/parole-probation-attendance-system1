import 'package:last/core/services/notification_service.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/activities/models/activity_model.dart';
import 'package:last/features/shared/models/notification_model.dart';
import 'package:uuid/uuid.dart';

class ActivityService {
  ActivityService({NotificationService? notificationService})
      : _notifications = notificationService ?? createNotificationService();

  final NotificationService _notifications;

  Stream<List<ActivityModel>> watchForOfficer(String officerId) {
    return watchAll(officerId: officerId);
  }

  Stream<List<ActivityModel>> watchForParolee(String paroleeId) {
    return watchAll().map((all) {
      final filtered = all.where((a) => a.isAssignedTo(paroleeId)).toList()
        ..sort((a, b) => a.scheduleDate.compareTo(b.scheduleDate));
      return filtered;
    });
  }

  Stream<List<ActivityModel>> watchAll({
    String? officerId,
    ActivityStatus? status,
    ActivityType? type,
  }) {
    if (!SupabaseConfig.isConfigured) return Stream.value([]);
    try {
      return SupabaseConfig.client
          .from('activities')
          .stream(primaryKey: ['id'])
          .asyncMap((rows) async {
            var list = await _processActivities(rows);
            if (officerId != null && officerId.isNotEmpty) {
              list = list.where((a) => a.assignedOfficerId == officerId).toList();
            }
            if (status != null) {
              list = list.where((a) => a.status == status).toList();
            }
            if (type != null) {
              list = list.where((a) => a.type == type).toList();
            }
            return list;
          })
          .handleError((Object e) {
            AppLogger.e('ActivityService', 'watchAll stream note', e);
          });
    } catch (e) {
      AppLogger.e('ActivityService', 'watchAll setup note', e);
      return Stream.value([]);
    }
  }

  Stream<ActivityStats> watchStats({String? officerId}) {
    return watchAll(officerId: officerId).map((list) {
      return ActivityStats(
        total: list.length,
        ongoing: list.where((a) => a.status == ActivityStatus.ongoing).length,
        completed: list.where((a) => a.status == ActivityStatus.completed).length,
        missed: list.where((a) => a.status == ActivityStatus.missed).length,
        pending: list.where((a) => a.status == ActivityStatus.pending).length,
      );
    });
  }

  Future<ActivityModel> createActivity({
    required String title,
    required ActivityType type,
    required String description,
    required String location,
    required DateTime scheduleDate,
    required String officerId,
    required String officerName,
    required List<String> paroleeIds,
  }) async {
    final id = const Uuid().v4();
    final now = DateTime.now();
    final activity = ActivityModel(
      id: id,
      title: title,
      type: type,
      description: description,
      location: location,
      scheduleDate: scheduleDate,
      assignedParoleeIds: paroleeIds,
      assignedOfficerId: officerId,
      status: ActivityStatus.pending,
      createdAt: now,
      completedParoleeIds: const [],
      officerName: officerName,
    );

    await SupabaseConfig.client.from('activities').insert({
      'id': id,
      'title': title,
      'activity_type': type.name,
      'description': description,
      'location_name': location,
      'start_time': scheduleDate.toIso8601String(),
      'end_time': scheduleDate.toIso8601String(),
      'assigned_parolees': paroleeIds,
      'assigned_officer_id': officerId,
      'officer_name': officerName,
      'status': ActivityStatus.pending.name,
      'completed_parolees': const <String>[],
      'created_at': now.toIso8601String(),
    });

    for (final pid in paroleeIds) {
      await _notifications.send(
        userId: pid,
        title: 'New Activity Assigned',
        body: '$title on ${scheduleDate.toLocal()}',
        type: NotificationType.reminder,
      );
    }
    return activity;
  }

  Future<void> updateStatus(String activityId, ActivityStatus status) async {
    await SupabaseConfig.client
        .from('activities')
        .update({'status': status.name})
        .eq('id', activityId);
  }

  Future<void> assignParolees(String activityId, List<String> paroleeIds) async {
    await SupabaseConfig.client
        .from('activities')
        .update({'assigned_parolees': paroleeIds})
        .eq('id', activityId);
  }

  Future<List<ActivityModel>> _processActivities(List<Map<String, dynamic>> rows) async {
    final now = DateTime.now();
    final list = <ActivityModel>[];

    for (final r in rows) {
      var activity = _mapRow(r);
      final originalStatus = activity.status;
      activity = _autoStatus(activity, now);
      if (activity.status != originalStatus && activity.id.isNotEmpty) {
        try {
          await SupabaseConfig.client
              .from('activities')
              .update({'status': activity.status.name})
              .eq('id', activity.id);
        } catch (_) {}
      }
      list.add(activity);
    }
    list.sort((a, b) => b.scheduleDate.compareTo(a.scheduleDate));
    return list;
  }

  ActivityModel _mapRow(Map<String, dynamic> r) {
    return ActivityModel.fromMap({
      'id': r['id']?.toString() ?? '',
      'title': r['title'] ?? '',
      'type': r['activity_type'] ?? r['type'] ?? 'other',
      'description': r['description'] ?? '',
      'location': r['location_name'] ?? r['location'] ?? '',
      'scheduleDate':
          r['start_time'] ?? r['schedule_date'] ?? r['scheduleDate'] ?? DateTime.now().toIso8601String(),
      'assignedParolees': r['assigned_parolees'] ?? r['assignedParolees'] ?? const [],
      'assignedOfficerId': r['assigned_officer_id'] ?? r['assignedOfficerId'] ?? '',
      'status': r['status'] ?? 'pending',
      'createdAt': r['created_at'] ?? r['createdAt'] ?? DateTime.now().toIso8601String(),
      'completedParolees': r['completed_parolees'] ?? r['completedParolees'] ?? const [],
      'officerName': r['officer_name'] ?? r['officerName'],
    });
  }

  ActivityModel _autoStatus(ActivityModel activity, DateTime now) {
    if (activity.status == ActivityStatus.completed) return activity;
    if (activity.scheduleDate.isBefore(now.subtract(const Duration(hours: 2))) &&
        activity.completedParoleeIds.length < activity.assignedParoleeIds.length) {
      return activity.copyWith(status: ActivityStatus.missed);
    }
    if (_isSameDay(activity.scheduleDate, now) && activity.status == ActivityStatus.pending) {
      return activity.copyWith(status: ActivityStatus.ongoing);
    }
    if (activity.completedParoleeIds.length >= activity.assignedParoleeIds.length &&
        activity.assignedParoleeIds.isNotEmpty) {
      return activity.copyWith(status: ActivityStatus.completed);
    }
    return activity;
  }

  bool _isSameDay(DateTime a, DateTime b) =>
      a.year == b.year && a.month == b.month && a.day == b.day;
}

ActivityService createActivityService() => ActivityService();
