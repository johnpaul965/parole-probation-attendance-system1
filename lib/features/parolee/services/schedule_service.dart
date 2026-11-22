import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/error_handler.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/shared/models/schedule_model.dart';

class ScheduleService {
  Stream<List<ScheduleModel>> watchForParolee(String paroleeId) {
    if (!SupabaseConfig.isConfigured || paroleeId.isEmpty) {
      return Stream.value([]);
    }
    try {
      return SupabaseConfig.client
          .from('schedules')
          .stream(primaryKey: ['id'])
          .eq('parolee_id', paroleeId)
          .map(_mapSchedules)
          .handleError((Object e) {
            AppLogger.e('ScheduleService', 'watchForParolee note', e);
          });
    } catch (e) {
      AppLogger.e('ScheduleService', 'watchForParolee setup note', e);
      return Stream.value([]);
    }
  }

  Stream<List<ScheduleModel>> watchForOfficer(String officerId) {
    if (!SupabaseConfig.isConfigured || officerId.isEmpty) {
      return Stream.value([]);
    }
    try {
      return SupabaseConfig.client
          .from('schedules')
          .stream(primaryKey: ['id'])
          .eq('officer_id', officerId)
          .map(_mapSchedules)
          .handleError((Object e) {
            AppLogger.e('ScheduleService', 'watchForOfficer note', e);
          });
    } catch (e) {
      AppLogger.e('ScheduleService', 'watchForOfficer setup note', e);
      return Stream.value([]);
    }
  }

  Future<ScheduleModel?> getNextForParolee(String paroleeId) async {
    try {
      final list = await watchForParolee(paroleeId).first;
      final now = DateTime.now();
      final upcoming = list.where((s) => s.scheduledAt.isAfter(now)).toList()
        ..sort((a, b) => a.scheduledAt.compareTo(b.scheduledAt));
      return upcoming.isEmpty ? null : upcoming.first;
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to load schedule');
    }
  }

  List<ScheduleModel> _mapSchedules(List<Map<String, dynamic>> rows) {
    final list = rows.map((r) {
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
    }).toList();
    list.sort((a, b) => a.scheduledAt.compareTo(b.scheduledAt));
    return list;
  }
}
