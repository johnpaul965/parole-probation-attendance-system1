import 'package:last/core/constants/tracking_config.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:uuid/uuid.dart';

/// Audit log for device assignments and officer tracking access.
class TrackingAuditService {
  Future<void> logDeviceAssignment({
    required String action,
    required String trackerId,
    String? paroleeId,
    String? actorId,
    String? actorRole,
    Map<String, dynamic>? metadata,
  }) async {
    if (!SupabaseConfig.isConfigured) return;
    try {
      await SupabaseConfig.client.from('tracking_audit_logs').insert({
        'id': const Uuid().v4(),
        'action': action,
        'resource_type': 'device',
        'resource_id': trackerId,
        'parolee_id': paroleeId,
        'actor_id': actorId,
        'actor_role': actorRole,
        'metadata': metadata,
        'created_at': DateTime.now().toIso8601String(),
      });
    } catch (e) {
      AppLogger.e('TrackingAuditService', 'logDeviceAssignment note', e);
    }
  }

  Future<void> logSupervisionPeriodUpdated({
    required String paroleeId,
    required String actorId,
    required String actorRole,
    Map<String, dynamic>? metadata,
  }) async {
    if (!SupabaseConfig.isConfigured) return;
    try {
      await SupabaseConfig.client.from('tracking_audit_logs').insert({
        'id': const Uuid().v4(),
        'action': 'supervision_period_updated',
        'resource_type': 'parolee',
        'resource_id': paroleeId,
        'parolee_id': paroleeId,
        'actor_id': actorId,
        'actor_role': actorRole,
        'metadata': metadata,
        'created_at': DateTime.now().toIso8601String(),
      });
    } catch (e) {
      AppLogger.e('TrackingAuditService', 'logSupervisionPeriodUpdated note', e);
    }
  }

  Future<void> logTrackingAccess({
    required String paroleeId,
    required String actorId,
    required String actorRole,
    required String endpoint,
  }) async {
    if (!SupabaseConfig.isConfigured) return;
    try {
      await SupabaseConfig.client.from('tracking_audit_logs').insert({
        'id': const Uuid().v4(),
        'action': 'tracking_access',
        'resource_type': 'parolee',
        'resource_id': paroleeId,
        'actor_id': actorId,
        'actor_role': actorRole,
        'metadata': {'endpoint': endpoint},
        'created_at': DateTime.now().toIso8601String(),
      });
    } catch (e) {
      AppLogger.e('TrackingAuditService', 'logTrackingAccess note', e);
    }
  }
}

/// Prevents duplicate alert spam with configurable cooldown.
class TrackingAlertCooldownService {
  TrackingAlertCooldownService({bool useSupabase = true})
      : _useSupabase = useSupabase && SupabaseConfig.isConfigured;

  final bool _useSupabase;
  final Map<String, DateTime> _localCache = {};

  Future<bool> shouldSendAlert({
    required String alertType,
    required String trackerId,
    String? paroleeId,
    Duration? cooldown,
  }) async {
    final window = cooldown ?? Duration(minutes: TrackingConfig.alertCooldownMinutes);
    final docId = '${alertType}_${trackerId}_${paroleeId ?? 'none'}';
    final now = DateTime.now();

    if (_useSupabase) {
      try {
        final rows = await SupabaseConfig.client
            .from('tracking_alert_cooldowns')
            .select()
            .eq('id', docId)
            .limit(1);
        if ((rows as List).isNotEmpty) {
          final row = Map<String, dynamic>.from(rows.first as Map);
          final lastSent = DateTime.tryParse((row['last_sent_at'] ?? row['lastSentAt'])?.toString() ?? '');
          if (lastSent != null && now.difference(lastSent) < window) {
            return false;
          }
        }
        await SupabaseConfig.client.from('tracking_alert_cooldowns').upsert({
          'id': docId,
          'last_sent_at': now.toIso8601String(),
          'alert_type': alertType,
        });
        return true;
      } catch (e) {
        AppLogger.e('TrackingAlertCooldownService', 'Supabase cooldown fallback', e);
      }
    }

    final cached = _localCache[docId];
    if (cached != null && now.difference(cached) < window) {
      return false;
    }
    _localCache[docId] = now;
    return true;
  }
}

TrackingAuditService createTrackingAuditService() => TrackingAuditService();

TrackingAlertCooldownService createTrackingAlertCooldownService() =>
    TrackingAlertCooldownService(useSupabase: SupabaseConfig.isConfigured);
