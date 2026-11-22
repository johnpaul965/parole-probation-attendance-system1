import 'package:last/core/services/session_service.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/activities/models/activity_model.dart';
import 'package:last/features/activities/models/activity_qr_payload.dart';
import 'package:uuid/uuid.dart';

class ActivityQrException implements Exception {
  ActivityQrException(this.message);
  final String message;
  @override
  String toString() => message;
}

class ActivityQrService {
  String _tokenDocId(String activityId, String paroleeId) => '${activityId}_$paroleeId';

  String? _currentUserId() {
    if (SupabaseConfig.isConfigured) {
      final sbUid = SupabaseConfig.client.auth.currentUser?.id;
      if (sbUid != null && sbUid.isNotEmpty) return sbUid;
    }
    return SessionService.getUser()?.id;
  }

  Future<ActivityQrPayload> generateForActivity({
    required ActivityModel activity,
    required String paroleeId,
    required String paroleeName,
    String? officerIdOverride,
    bool rotate = false,
  }) async {
    AppLogger.d('ActivityQrService', 'generateForActivity', '${activity.id} / $paroleeId');

    final uid = _currentUserId();
    if (uid == null || uid.isEmpty) {
      throw ActivityQrException('You are not signed in. Sign in again to view your QR code.');
    }
    if (paroleeId.isEmpty) {
      throw ActivityQrException('Parolee account ID is missing. Sign out and sign in again.');
    }
    if (paroleeId != uid) {
      throw ActivityQrException('This QR can only be generated for the signed-in parolee account.');
    }

    if (!activity.isAssignedTo(uid)) {
      throw ActivityQrException('You are not assigned to this activity.');
    }
    if (activity.status == ActivityStatus.missed) {
      throw ActivityQrException('This activity was marked missed. QR is no longer available.');
    }
    if (activity.status == ActivityStatus.completed) {
      throw ActivityQrException('This activity is completed. QR is no longer available.');
    }

    final officerId = _resolveOfficerId(activity, officerIdOverride);
    if (officerId.isEmpty) {
      throw ActivityQrException(
        'No supervising officer is linked to this activity. Contact your PPA office.',
      );
    }

    if (!SupabaseConfig.isConfigured) {
      return _localPayload(
        activity: activity,
        paroleeId: uid,
        paroleeName: paroleeName,
        officerId: officerId,
      );
    }

    final docId = _tokenDocId(activity.id, uid);
    final now = DateTime.now();

    try {
      if (!rotate) {
        final existingRows = await SupabaseConfig.client
            .from('activity_qr_tokens')
            .select()
            .eq('id', docId)
            .limit(1);
        if ((existingRows as List).isNotEmpty) {
          final reused = _payloadFromRow(
            Map<String, dynamic>.from(existingRows.first as Map),
            activity: activity,
            paroleeId: uid,
            paroleeName: paroleeName,
            officerId: officerId,
            fallbackTime: now,
          );
          if (reused != null) {
            AppLogger.d('ActivityQrService', 'Reusing saved token', docId);
            return reused;
          }
        }
      }

      final token = const Uuid().v4();
      await SupabaseConfig.client.from('activity_qr_tokens').upsert({
        'id': docId,
        'activity_id': activity.id,
        'parolee_id': uid,
        'parolee_name': paroleeName,
        'officer_id': officerId,
        'assigned_officer_id': officerId,
        'activity_title': activity.title,
        'token': token,
        'issued_at': now.toIso8601String(),
      });

      final payload = ActivityQrPayload(
        paroleeId: uid,
        paroleeName: paroleeName,
        activityId: activity.id,
        assignedOfficerId: officerId,
        activityTitle: activity.title,
        token: token,
        timestamp: now,
      );
      payload.encode();
      return payload;
    } catch (e) {
      AppLogger.e('ActivityQrService', 'Supabase QR token fallback', e);
      return _localPayload(
        activity: activity,
        paroleeId: uid,
        paroleeName: paroleeName,
        officerId: officerId,
      );
    }
  }

  ActivityQrPayload? _payloadFromRow(
    Map<String, dynamic> data, {
    required ActivityModel activity,
    required String paroleeId,
    required String paroleeName,
    required String officerId,
    required DateTime fallbackTime,
  }) {
    final token = data['token'] as String? ?? '';
    final storedParolee = (data['parolee_id'] ?? data['paroleeId']) as String? ?? '';
    final storedActivity = (data['activity_id'] ?? data['activityId']) as String? ?? '';
    if (token.isEmpty || storedParolee != paroleeId || storedActivity != activity.id) {
      return null;
    }
    return ActivityQrPayload(
      paroleeId: paroleeId,
      paroleeName: paroleeName,
      activityId: activity.id,
      assignedOfficerId: officerId,
      activityTitle: activity.title,
      token: token,
      timestamp: DateTime.tryParse((data['issued_at'] ?? data['issuedAt']) as String? ?? '') ?? fallbackTime,
    );
  }

  String _resolveOfficerId(ActivityModel activity, String? override) {
    if (override != null && override.isNotEmpty) return override;
    if (activity.assignedOfficerId.isNotEmpty) return activity.assignedOfficerId;
    return '';
  }

  ActivityQrPayload _localPayload({
    required ActivityModel activity,
    required String paroleeId,
    required String paroleeName,
    required String officerId,
  }) {
    AppLogger.d('ActivityQrService', 'Local QR payload');
    return ActivityQrPayload(
      paroleeId: paroleeId,
      paroleeName: paroleeName,
      activityId: activity.id,
      assignedOfficerId: officerId,
      activityTitle: activity.title,
      token: const Uuid().v4(),
      timestamp: DateTime.now(),
    );
  }

  Future<bool> validateToken(ActivityQrPayload payload) async {
    final resolved = await resolveFromScan(
      token: payload.token,
      activityId: payload.activityId,
    );
    if (resolved == null) {
      return payload.token.isNotEmpty;
    }
    return payload.paroleeId.isEmpty || resolved.paroleeId == payload.paroleeId;
  }

  /// Looks up a scanned token in Supabase `activity_qr_tokens`.
  Future<ActivityQrPayload?> resolveFromScan({
    required String token,
    required String activityId,
  }) async {
    if (token.isEmpty || !SupabaseConfig.isConfigured) return null;

    try {
      final query = activityId.isNotEmpty
          ? SupabaseConfig.client
              .from('activity_qr_tokens')
              .select()
              .eq('token', token)
              .eq('activity_id', activityId)
              .limit(1)
          : SupabaseConfig.client
              .from('activity_qr_tokens')
              .select()
              .eq('token', token)
              .limit(1);
      final rows = await query;
      if ((rows as List).isEmpty) return null;
      final data = Map<String, dynamic>.from(rows.first as Map);
      return ActivityQrPayload(
        paroleeId: (data['parolee_id'] ?? data['paroleeId']) as String? ?? '',
        paroleeName: (data['parolee_name'] ?? data['paroleeName']) as String? ?? '',
        activityId: (data['activity_id'] ?? data['activityId']) as String? ?? activityId,
        assignedOfficerId:
            (data['assigned_officer_id'] ?? data['assignedOfficerId'] ?? data['officer_id']) as String? ?? '',
        activityTitle: (data['activity_title'] ?? data['activityTitle']) as String? ?? '',
        token: data['token'] as String? ?? token,
        timestamp: DateTime.tryParse((data['issued_at'] ?? data['issuedAt']) as String? ?? '') ?? DateTime.now(),
      );
    } catch (e) {
      AppLogger.e('ActivityQrService', 'resolveFromScan note', e);
      return null;
    }
  }
}
