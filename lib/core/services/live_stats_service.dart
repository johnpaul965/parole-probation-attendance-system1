import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/officer/models/live_stats.dart';
import 'package:last/features/officer/models/parolee_summary.dart';
import 'package:last/features/shared/models/attendance_record.dart';

class LiveStatsService {
  Future<void> publish(String officerId, List<ParoleeSummary> parolees) async {
    if (!SupabaseConfig.isConfigured || officerId.isEmpty) return;
    try {
      final live = LiveStats.fromDashboard(parolees);
      await SupabaseConfig.client.from('live_stats').upsert({
        'officer_id': officerId,
        'total_cases': live.stats.totalCases,
        'checked_in': live.stats.checkedIn,
        'pending': live.stats.pending,
        'missed': live.stats.missed,
        'updated_at': (live.updatedAt ?? DateTime.now()).toIso8601String(),
        'checked_in_list': live.toMap()['checkedInList'],
        'pending_list': live.toMap()['pendingList'],
        'missed_list': live.toMap()['missedList'],
      });
      AppLogger.d('LiveStatsService', 'Published live_stats for $officerId');
    } catch (e, st) {
      AppLogger.e('LiveStatsService', 'publish note (non-fatal)', e, st);
    }
  }

  Future<void> syncParoleeStatus({
    required String paroleeId,
    required AttendanceStatus status,
  }) async {
    if (!SupabaseConfig.isConfigured || paroleeId.isEmpty) return;
    try {
      await SupabaseConfig.client.from('profiles').update({
        'today_status': status.name,
        'status_updated_at': DateTime.now().toIso8601String(),
      }).eq('id', paroleeId);
    } catch (e, st) {
      AppLogger.e('LiveStatsService', 'syncParoleeStatus note', e, st);
    }
  }

  Stream<LiveStats?> watchOfficer(String officerId) {
    if (!SupabaseConfig.isConfigured || officerId.isEmpty) {
      return Stream.value(null);
    }
    try {
      return SupabaseConfig.client
          .from('live_stats')
          .stream(primaryKey: ['officer_id'])
          .eq('officer_id', officerId)
          .map((rows) {
            if (rows.isEmpty) return null;
            try {
              return LiveStats.fromMap(rows.first);
            } catch (e, st) {
              AppLogger.e('LiveStatsService', 'Parse live_stats failed', e, st);
              return null;
            }
          })
          .handleError((Object e, StackTrace st) {
            AppLogger.e('LiveStatsService', 'watchOfficer stream note', e, st);
          });
    } catch (e, st) {
      AppLogger.e('LiveStatsService', 'watchOfficer setup note', e, st);
      return Stream.value(null);
    }
  }

  Stream<List<LiveStats>> watchAll() {
    if (!SupabaseConfig.isConfigured) {
      return Stream.value([]);
    }
    try {
      return SupabaseConfig.client
          .from('live_stats')
          .stream(primaryKey: ['officer_id'])
          .map((rows) {
            final list = <LiveStats>[];
            for (final row in rows) {
              try {
                list.add(LiveStats.fromMap(row));
              } catch (e) {
                AppLogger.e('LiveStatsService', 'Skip invalid live_stats', e);
              }
            }
            return list;
          })
          .handleError((Object e, StackTrace st) {
            AppLogger.e('LiveStatsService', 'watchAll stream note', e, st);
          });
    } catch (e, st) {
      AppLogger.e('LiveStatsService', 'watchAll setup note', e, st);
      return Stream.value([]);
    }
  }
}
