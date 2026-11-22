import 'dart:async';

import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/error_handler.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/auth/models/app_user.dart';
import 'package:last/features/shared/models/attendance_record.dart';

abstract class AdminService {
  Stream<List<AppUser>> watchAllUsers();
  Future<List<AppUser>> getAllUsers();
  Future<void> assignOfficer(String paroleeId, String officerId);
  Future<void> removeOfficer(String paroleeId);
  Future<void> toggleUserActive(String userId, bool active);
  Future<void> createUser(AppUser user);
  Future<void> deleteUser(String userId);
  Future<double> computeComplianceRate();
  Stream<double> watchComplianceRate();
}

class SupabaseAdminService implements AdminService {
  @override
  Stream<List<AppUser>> watchAllUsers() {
    if (!SupabaseConfig.isConfigured) {
      return Stream.value([]);
    }
    try {
      return SupabaseConfig.client
          .from('profiles')
          .stream(primaryKey: ['id'])
          .map((rows) => rows.map(_mapUser).toList())
          .handleError((Object e) {
            AppLogger.e('AdminService', 'watchAllUsers stream note', e);
          });
    } catch (e) {
      AppLogger.e('AdminService', 'watchAllUsers setup note', e);
      return Stream.value([]);
    }
  }

  @override
  Future<List<AppUser>> getAllUsers() async {
    if (!SupabaseConfig.isConfigured) return [];
    try {
      final rows = await SupabaseConfig.client.from('profiles').select();
      return (rows as List)
          .map((d) => _mapUser(Map<String, dynamic>.from(d as Map)))
          .toList();
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to load users');
    }
  }

  @override
  Future<void> assignOfficer(String paroleeId, String officerId) async {
    try {
      await SupabaseConfig.client.from('profiles').update({
        'officer_id': officerId,
      }).eq('id', paroleeId);
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to assign officer');
    }
  }

  @override
  Future<void> removeOfficer(String paroleeId) async {
    try {
      await SupabaseConfig.client.from('profiles').update({
        'officer_id': null,
      }).eq('id', paroleeId);
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to remove officer assignment');
    }
  }

  @override
  Future<void> toggleUserActive(String userId, bool active) async {
    try {
      await SupabaseConfig.client
          .from('profiles')
          .update({'is_active': active})
          .eq('id', userId);
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to update user status');
    }
  }

  @override
  Future<void> createUser(AppUser user) async {
    try {
      await SupabaseConfig.client.from('profiles').upsert({
        'id': user.id,
        'email': user.email,
        'full_name': user.fullName,
        'role': user.role.name,
        'department': user.department,
        'case_number': user.caseNumber,
        'officer_id': user.officerId,
        'tracker_id': user.trackerId,
        'avatar_initials': user.avatarInitials,
        'phone': user.phone,
        'is_active': user.isActive,
        'supervision_start_date': user.supervisionStartDate,
        'supervision_end_date': user.supervisionEndDate,
        'supervision_is_suspended': user.supervisionIsSuspended,
      });
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to create user');
    }
  }

  @override
  Future<void> deleteUser(String userId) async {
    try {
      try {
        await SupabaseConfig.client.from('gps_trackers').update({
          'parolee_id': null,
          'parolee_name': null,
          'status': 'unassigned',
        }).eq('parolee_id', userId);
      } catch (_) {}
      await SupabaseConfig.client.from('profiles').delete().eq('id', userId);
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to remove user');
    }
  }

  @override
  Future<double> computeComplianceRate() async {
    if (!SupabaseConfig.isConfigured) return 0;
    try {
      final rows = await SupabaseConfig.client.from('attendance_records').select();
      if ((rows as List).isEmpty) return 0;

      final records = rows.map((d) {
        final r = Map<String, dynamic>.from(d as Map);
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
      }).toList();

      final last30 = records.where((r) {
        final t = r.checkInTime ?? r.scheduledTime;
        if (t == null) return false;
        return t.isAfter(DateTime.now().subtract(const Duration(days: 30)));
      }).toList();

      if (last30.isEmpty) return 0;
      final compliant = last30.where((r) => r.status != AttendanceStatus.missed).length;
      return (compliant / last30.length) * 100;
    } catch (e) {
      AppLogger.e('AdminService', 'computeComplianceRate note', e);
      return 0;
    }
  }

  @override
  Stream<double> watchComplianceRate() {
    if (!SupabaseConfig.isConfigured) {
      return Stream.value(0);
    }
    try {
      return SupabaseConfig.client
          .from('attendance_records')
          .stream(primaryKey: ['id'])
          .asyncMap((_) => computeComplianceRate())
          .handleError((Object e) {
            AppLogger.e('AdminService', 'watchComplianceRate note', e);
          });
    } catch (e) {
      AppLogger.e('AdminService', 'watchComplianceRate setup note', e);
      return Stream.value(0);
    }
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

AdminService createAdminService() => SupabaseAdminService();
