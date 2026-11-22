import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/core/utils/ticket_generator.dart';
import 'package:last/features/auth/models/app_user.dart';
import 'package:last/features/shared/models/attendance_record.dart';
import 'package:uuid/uuid.dart';

abstract class AttendanceService {
  Stream<List<AttendanceRecord>> watchAttendance({String? officerId, String? paroleeId});
  Future<List<AttendanceRecord>> getAttendance({String? officerId, String? paroleeId});
  Future<AttendanceRecord> createCheckIn(AppUser parolee, {String? location});
  Future<AttendanceRecord> verifyQr(String qrPayload, AppUser officer);
  Future<void> updateStatus(String id, AttendanceStatus status);
}

class SupabaseAttendanceService implements AttendanceService {
  final SupabaseClient _supabase = SupabaseConfig.client;
  final List<AttendanceRecord> _local = [];

  @override
  Stream<List<AttendanceRecord>> watchAttendance({String? officerId, String? paroleeId}) {
    if (SupabaseConfig.isConfigured) {
      try {
        return _supabase
            .from('attendance_records')
            .stream(primaryKey: ['id'])
            .map((rows) {
              var records = rows.map(_mapRow).toList();
              if (officerId != null && officerId.isNotEmpty) {
                records = records.where((r) => r.officerId == officerId).toList();
              }
              if (paroleeId != null && paroleeId.isNotEmpty) {
                records = records.where((r) => r.paroleeId == paroleeId).toList();
              }
              records.sort((a, b) => (b.checkInTime ?? b.scheduledTime ?? DateTime(2000))
                  .compareTo(a.checkInTime ?? a.scheduledTime ?? DateTime(2000)));
              return records;
            });
      } catch (_) {}
    }
    return Stream.value(_local);
  }

  @override
  Future<List<AttendanceRecord>> getAttendance({String? officerId, String? paroleeId}) async {
    if (SupabaseConfig.isConfigured) {
      try {
        final rows = await _supabase.from('attendance_records').select();
        var records = (rows as List)
            .map((r) => _mapRow(Map<String, dynamic>.from(r as Map)))
            .toList();
        if (officerId != null && officerId.isNotEmpty) {
          records = records.where((r) => r.officerId == officerId).toList();
        }
        if (paroleeId != null && paroleeId.isNotEmpty) {
          records = records.where((r) => r.paroleeId == paroleeId).toList();
        }
        return records;
      } catch (_) {}
    }
    return _local;
  }

  @override
  Future<AttendanceRecord> createCheckIn(AppUser parolee, {String? location}) async {
    final ticketId = TicketGenerator.generate();
    final rec = AttendanceRecord(
      id: const Uuid().v4(),
      ticketId: ticketId,
      paroleeId: parolee.id,
      paroleeName: parolee.fullName,
      officerId: parolee.officerId ?? '',
      status: AttendanceStatus.pending,
      scheduledTime: DateTime.now(),
      location: location ?? 'PPA Tacloban Office',
      verified: false,
    );
    _local.insert(0, rec);

    if (SupabaseConfig.isConfigured) {
      try {
        await _supabase.from('attendance_records').insert({
          'id': rec.id,
          'ticket_id': rec.ticketId,
          'parolee_id': rec.paroleeId,
          'parolee_name': rec.paroleeName,
          'officer_id': rec.officerId,
          'status': rec.status.name,
          'location': rec.location,
          'verified': false,
        });
      } catch (_) {}
    }
    return rec;
  }

  @override
  Future<AttendanceRecord> verifyQr(String qrPayload, AppUser officer) async {
    final ticketId = TicketGenerator.extractTicketId(qrPayload) ?? TicketGenerator.generate();
    final rec = AttendanceRecord(
      id: const Uuid().v4(),
      ticketId: ticketId,
      paroleeId: 'demo-parolee',
      paroleeName: 'Juan Dela Cruz',
      officerId: officer.id,
      status: AttendanceStatus.confirmed,
      checkInTime: DateTime.now(),
      location: 'PPA Tacloban Office',
      verified: true,
    );
    _local.insert(0, rec);

    if (SupabaseConfig.isConfigured) {
      try {
        await _supabase.from('attendance_records').insert({
          'id': rec.id,
          'ticket_id': rec.ticketId,
          'parolee_id': rec.paroleeId,
          'parolee_name': rec.paroleeName,
          'officer_id': rec.officerId,
          'status': rec.status.name,
          'check_in_time': rec.checkInTime?.toIso8601String(),
          'location': rec.location,
          'verified': true,
        });
      } catch (_) {}
    }
    return rec;
  }

  @override
  Future<void> updateStatus(String id, AttendanceStatus status) async {
    if (SupabaseConfig.isConfigured) {
      try {
        await _supabase.from('attendance_records').update({'status': status.name}).eq('id', id);
      } catch (_) {}
    }
  }

  AttendanceRecord _mapRow(Map<String, dynamic> r) {
    return AttendanceRecord(
      id: r['id']?.toString() ?? '',
      ticketId: r['ticket_id']?.toString() ?? r['ticketId']?.toString() ?? '',
      paroleeId: r['parolee_id']?.toString() ?? r['paroleeId']?.toString() ?? '',
      paroleeName: r['parolee_name']?.toString() ?? r['paroleeName']?.toString() ?? 'Parolee',
      officerId: r['officer_id']?.toString() ?? r['officerId']?.toString() ?? '',
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
}

AttendanceService createAttendanceService() => SupabaseAttendanceService();
