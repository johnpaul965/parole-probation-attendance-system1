import 'dart:async';

import 'package:intl/intl.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/date_utils.dart';
import 'package:last/core/utils/error_handler.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/auth/models/app_user.dart';
import 'package:last/features/auth/models/user_role.dart';
import 'package:last/features/reports/models/report_data.dart';
import 'package:last/features/shared/models/attendance_record.dart';

abstract class ReportsService {
  Future<ReportData> getReportData({String? officerId});
  Stream<ReportData> watchReportData({String? officerId});
}

class SupabaseReportsService implements ReportsService {
  @override
  Stream<ReportData> watchReportData({String? officerId}) {
    if (!SupabaseConfig.isConfigured) {
      return Stream.value(_emptyReport());
    }
    try {
      return SupabaseConfig.client
          .from('attendance_records')
          .stream(primaryKey: ['id'])
          .asyncMap((_) => getReportData(officerId: officerId))
          .handleError((Object e) {
            AppLogger.e('ReportsService', 'watchReportData stream note', e);
          });
    } catch (e) {
      AppLogger.e('ReportsService', 'watchReportData setup note', e);
      return Stream.value(_emptyReport());
    }
  }

  ReportData _emptyReport() => ReportData(
        weeklyPoints: const [],
        monthlyPoints: const [],
        complianceRate: 0,
        attendancePercentage: 0,
        missedCount: 0,
        totalCheckIns: 0,
        pendingCount: 0,
        totalAttendance: 0,
        recentRecords: const [],
        officerPerformance: const [],
        generatedAt: DateTime.now(),
      );

  @override
  Future<ReportData> getReportData({String? officerId}) async {
    if (!SupabaseConfig.isConfigured) return _emptyReport();
    try {
      final attQuery = officerId != null
          ? SupabaseConfig.client.from('attendance_records').select().eq('officer_id', officerId)
          : SupabaseConfig.client.from('attendance_records').select();
      final attRows = await attQuery;
      final records = (attRows as List)
          .map((d) => _mapAttendance(Map<String, dynamic>.from(d as Map)))
          .toList();

      List<AppUser> users = [];
      try {
        final userRows = await SupabaseConfig.client.from('profiles').select();
        users = (userRows as List)
            .map((d) => _mapUser(Map<String, dynamic>.from(d as Map)))
            .toList();
      } catch (e) {
        AppLogger.e('ReportsService', 'profiles fetch note', e);
      }

      var parolees = users.where((u) => u.role == UserRole.parolee).toList();
      if (officerId != null) {
        parolees = parolees.where((p) => p.officerId == officerId).toList();
      }

      final checkedIn = records
          .where((r) =>
              r.status == AttendanceStatus.checkedIn ||
              r.status == AttendanceStatus.confirmed)
          .length;
      final missed = records.where((r) => r.status == AttendanceStatus.missed).length;
      final total = records.length;

      final todayCheckedParolees = records
          .where((r) => AppDateUtils.isToday(r.checkInTime))
          .map((r) => r.paroleeId)
          .toSet();
      final pending = parolees.where((p) => !todayCheckedParolees.contains(p.id)).length;

      final complianceRate = total == 0 ? 0.0 : ((total - missed) / total) * 100;
      final attendancePct =
          parolees.isEmpty ? 0.0 : (todayCheckedParolees.length / parolees.length) * 100;

      return ReportData(
        weeklyPoints: _buildWeeklyPoints(records),
        monthlyPoints: _buildMonthlyPoints(records),
        complianceRate: complianceRate,
        attendancePercentage: attendancePct,
        missedCount: missed,
        totalCheckIns: checkedIn,
        pendingCount: pending,
        totalAttendance: total,
        recentRecords: records.take(20).toList(),
        officerPerformance: await _buildOfficerPerformance(users, records),
        generatedAt: DateTime.now(),
      );
    } catch (e) {
      throw ErrorHandler.wrap(e, 'Failed to generate report');
    }
  }

  List<ChartPoint> _buildWeeklyPoints(List<AttendanceRecord> records) {
    final now = DateTime.now();
    final labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
    final points = <ChartPoint>[];

    for (var i = 6; i >= 0; i--) {
      final day = DateTime(now.year, now.month, now.day - i);
      final count = records.where((r) {
        final t = r.checkInTime;
        return t != null && AppDateUtils.isSameDay(t, day);
      }).length;
      points.add(ChartPoint(label: labels[day.weekday - 1], value: count.toDouble()));
    }
    return points;
  }

  List<ChartPoint> _buildMonthlyPoints(List<AttendanceRecord> records) {
    final now = DateTime.now();
    final fmt = DateFormat('MMM');
    final points = <ChartPoint>[];

    for (var i = 11; i >= 0; i--) {
      final month = DateTime(now.year, now.month - i, 1);
      final count = records.where((r) {
        final t = r.checkInTime ?? r.scheduledTime;
        return t != null && t.year == month.year && t.month == month.month;
      }).length;
      points.add(ChartPoint(label: fmt.format(month), value: count.toDouble()));
    }
    return points;
  }

  Future<List<OfficerPerformance>> _buildOfficerPerformance(
    List<AppUser> users,
    List<AttendanceRecord> records,
  ) async {
    final officers = users.where((u) => u.role == UserRole.officer).toList();
    final performance = <OfficerPerformance>[];

    for (final officer in officers) {
      final officerRecords = records.where((r) => r.officerId == officer.id).toList();
      final paroleeCount =
          users.where((u) => u.role == UserRole.parolee && u.officerId == officer.id).length;
      final checkIns = officerRecords
          .where((r) =>
              r.status == AttendanceStatus.checkedIn ||
              r.status == AttendanceStatus.confirmed)
          .length;
      final missed = officerRecords.where((r) => r.status == AttendanceStatus.missed).length;
      final total = officerRecords.length;
      final rate = total == 0 ? 0.0 : ((total - missed) / total) * 100;

      performance.add(OfficerPerformance(
        officerId: officer.id,
        officerName: officer.fullName,
        totalParolees: paroleeCount,
        checkIns: checkIns,
        missed: missed,
        complianceRate: rate,
      ));
    }
    return performance;
  }

  AttendanceRecord _mapAttendance(Map<String, dynamic> r) {
    return AttendanceRecord(
      id: r['id']?.toString() ?? '',
      ticketId: (r['ticket_id'] ?? r['ticketId'])?.toString() ?? '',
      paroleeId: (r['parolee_id'] ?? r['paroleeId'])?.toString() ?? '',
      paroleeName: (r['parolee_name'] ?? r['paroleeName'])?.toString() ?? 'Parolee',
      officerId: (r['officer_id'] ?? r['officerId'])?.toString() ?? '',
      status: AttendanceStatus.fromString(r['status']?.toString() ?? 'checkedIn'),
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

ReportsService createReportsService() => SupabaseReportsService();
