import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:last/core/utils/app_logger.dart';
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
    final now = DateTime.now();
    var rec = AttendanceRecord(
      id: const Uuid().v4(),
      ticketId: ticketId,
      paroleeId: parolee.id,
      paroleeName: parolee.fullName,
      officerId: parolee.officerId ?? '',
      status: AttendanceStatus.pending,
      scheduledTime: now,
      location: location ?? 'PPA Tacloban Office',
      verified: false,
    );
    _local.insert(0, rec);

    if (SupabaseConfig.isConfigured) {
      try {
        final payload = <String, dynamic>{
          'ticket_id': rec.ticketId,
          'parolee_id': rec.paroleeId,
          'parolee_name': rec.paroleeName,
          if (rec.officerId.isNotEmpty) 'officer_id': rec.officerId,
          'status': rec.status.name,
          'scheduled_time': now.toIso8601String(),
          'location': rec.location,
          'verified': false,
          'record_type': 'checkin',
        };
        final inserted = await _supabase
            .from('attendance_records')
            .insert(payload)
            .select('id')
            .limit(1);
        if ((inserted as List).isNotEmpty) {
          final dbId = (inserted.first as Map)['id']?.toString();
          if (dbId != null && dbId.isNotEmpty) {
            rec = rec.copyWith(id: dbId);
          }
        }
      } catch (e) {
        AppLogger.e('AttendanceService', 'createCheckIn insert failed', e);
      }
    }
    return rec;
  }

  @override
  Future<AttendanceRecord> verifyQr(String qrPayload, AppUser officer) async {
    final ticketId = TicketGenerator.extractTicketId(qrPayload) ?? TicketGenerator.generate();
    final now = DateTime.now();
    String paroleeId = '';
    String paroleeName = 'Parolee';

    if (SupabaseConfig.isConfigured) {
      try {
        final existing = await _supabase
            .from('attendance_records')
            .select()
            .eq('ticket_id', ticketId)
            .limit(1);
        if ((existing as List).isNotEmpty) {
          final row = Map<String, dynamic>.from(existing.first as Map);
          final existingId = row['id'];
          paroleeId = (row['parolee_id'] ?? row['paroleeId'])?.toString() ?? '';
          paroleeName = (row['parolee_name'] ?? row['paroleeName'])?.toString() ?? 'Parolee';
          await _supabase.from('attendance_records').update({
            'officer_id': officer.id,
            'status': AttendanceStatus.confirmed.name,
            'check_in_time': now.toIso8601String(),
            'verified': true,
          }).eq('id', existingId);

          final updated = AttendanceRecord(
            id: existingId.toString(),
            ticketId: ticketId,
            paroleeId: paroleeId,
            paroleeName: paroleeName,
            officerId: officer.id,
            status: AttendanceStatus.confirmed,
            checkInTime: now,
            location: row['location']?.toString() ?? 'PPA Tacloban Office',
            verified: true,
          );
          _local.insert(0, updated);
          return updated;
        }
      } catch (e) {
        AppLogger.e('AttendanceService', 'verifyQr lookup failed', e);
      }
    }

    var rec = AttendanceRecord(
      id: const Uuid().v4(),
      ticketId: ticketId,
      paroleeId: paroleeId,
      paroleeName: paroleeName,
      officerId: officer.id,
      status: AttendanceStatus.confirmed,
      checkInTime: now,
      location: 'PPA Tacloban Office',
      verified: true,
    );
    _local.insert(0, rec);

    if (SupabaseConfig.isConfigured) {
      try {
        final inserted = await _supabase
            .from('attendance_records')
            .insert({
              'ticket_id': rec.ticketId,
              if (rec.paroleeId.isNotEmpty) 'parolee_id': rec.paroleeId,
              'parolee_name': rec.paroleeName,
              'officer_id': rec.officerId,
              'status': rec.status.name,
              'check_in_time': now.toIso8601String(),
              'location': rec.location,
              'verified': true,
              'record_type': 'checkin',
            })
            .select('id')
            .limit(1);
        if ((inserted as List).isNotEmpty) {
          final dbId = (inserted.first as Map)['id']?.toString();
          if (dbId != null && dbId.isNotEmpty) {
            rec = rec.copyWith(id: dbId);
          }
        }
      } catch (e) {
        AppLogger.e('AttendanceService', 'verifyQr insert failed', e);
      }
    }
    return rec;
  }

  @override
  Future<void> updateStatus(String id, AttendanceStatus status) async {
    if (SupabaseConfig.isConfigured) {
      try {
        await _supabase.from('attendance_records').update({'status': status.name}).eq('id', id);
      } catch (e) {
        AppLogger.e('AttendanceService', 'updateStatus failed', e);
      }
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
