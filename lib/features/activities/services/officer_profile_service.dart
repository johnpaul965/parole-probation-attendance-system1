import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/auth/models/officer_profile.dart';

class OfficerProfileService {
  Future<OfficerProfile?> getById(String officerId) async {
    if (!SupabaseConfig.isConfigured || officerId.isEmpty) return null;
    try {
      final rows = await SupabaseConfig.client
          .from('profiles')
          .select()
          .eq('id', officerId)
          .limit(1);
      if ((rows as List).isEmpty) return null;
      return _mapProfile(officerId, Map<String, dynamic>.from(rows.first as Map));
    } catch (e) {
      AppLogger.e('OfficerProfileService', 'getById note', e);
      return null;
    }
  }

  Stream<OfficerProfile?> watchById(String officerId) {
    if (!SupabaseConfig.isConfigured || officerId.isEmpty) {
      return Stream.value(null);
    }
    try {
      return SupabaseConfig.client
          .from('profiles')
          .stream(primaryKey: ['id'])
          .eq('id', officerId)
          .map((rows) {
            if (rows.isEmpty) return null;
            return _mapProfile(officerId, rows.first);
          })
          .handleError((Object e) {
            AppLogger.e('OfficerProfileService', 'watchById note', e);
          });
    } catch (e) {
      AppLogger.e('OfficerProfileService', 'watchById setup note', e);
      return Stream.value(null);
    }
  }

  OfficerProfile _mapProfile(String id, Map<String, dynamic> m) {
    return OfficerProfile.fromMap(id, {
      'name': m['full_name'] ?? m['fullName'] ?? m['name'] ?? '',
      'email': m['email'] ?? '',
      'contact': m['phone'] ?? m['contact'],
      'office': m['department'] ?? m['office'],
      'profileImage': m['profile_image'] ?? m['profileImage'],
    });
  }
}
