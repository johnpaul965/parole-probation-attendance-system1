import 'package:supabase_flutter/supabase_flutter.dart';

class AppException implements Exception {
  AppException(this.message, {this.code});
  final String message;
  final String? code;

  @override
  String toString() => message;
}

class ErrorHandler {
  static String friendlyMessage(Object error) {
    if (error is AppException) return error.message;
    if (error is PostgrestException) return _mapPostgrestError(error);
    if (error is AuthException) return error.message;
    final msg = error.toString();
    if (msg.contains('permission denied') || msg.contains('42501')) {
      return 'Access denied. Check your Supabase Row Level Security (RLS) policies.';
    }
    if (msg.contains('SocketException') || msg.contains('network') || msg.contains('unavailable')) {
      return 'Network unavailable. Check your connection and try again.';
    }
    return msg.replaceFirst('Exception: ', '');
  }

  static String _mapPostgrestError(PostgrestException e) {
    if (e.code == '42501' || e.message.toLowerCase().contains('permission')) {
      return 'Access denied. Check your Supabase RLS policies.';
    }
    if (e.code == '23505') {
      return 'This record already exists.';
    }
    if (e.code == 'PGRST116') {
      return 'Requested record was not found.';
    }
    return e.message.isNotEmpty ? e.message : 'Supabase error (${e.code ?? 'unknown'})';
  }

  static AppException wrap(Object error, [String? context]) {
    final msg = friendlyMessage(error);
    final prefix = context != null ? '$context: ' : '';
    return AppException('$prefix$msg');
  }
}
