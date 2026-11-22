import 'dart:async';
import 'package:supabase_flutter/supabase_flutter.dart' hide AuthException;
import 'package:last/core/services/session_service.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/auth/models/app_user.dart';
import 'package:last/features/auth/models/user_role.dart';
import 'package:last/features/auth/services/auth_service.dart';
import 'package:last/features/auth/services/demo_auth_service.dart';

class SupabaseAuthService implements AuthService {
  final SupabaseClient _supabase = SupabaseConfig.client;
  final DemoAuthService _demoFallback = DemoAuthService();
  AppUser? _currentUser;

  @override
  AppUser? get currentUser => _currentUser;

  @override
  Stream<AppUser?> get authStateChanges {
    final controller = StreamController<AppUser?>.broadcast();

    try {
      final cur = _supabase.auth.currentUser;
      if (cur != null) {
        _currentUser = _mapUser(cur);
        controller.add(_currentUser);
        unawaited(_hydrateProfile(cur.id, controller));
      } else {
        final saved = SessionService.getUser();
        if (saved != null && saved.id.startsWith('demo-')) {
          _currentUser = saved;
          controller.add(saved);
        } else {
          controller.add(null);
        }
      }
    } catch (_) {
      controller.add(null);
    }

    try {
      _supabase.auth.onAuthStateChange.listen((data) {
        final user = data.session?.user;
        if (user == null) {
          _currentUser = null;
          controller.add(null);
        } else {
          _currentUser = _mapUser(user);
          controller.add(_currentUser);
          unawaited(_hydrateProfile(user.id, controller));
        }
      }, onError: (_) {
        controller.add(null);
      });
    } catch (_) {}

    return controller.stream;
  }

  Future<void> _hydrateProfile(
    String userId,
    StreamController<AppUser?> controller,
  ) async {
    final cur = _supabase.auth.currentUser;
    if (cur == null || cur.id != userId) return;
    final hydrated = await _loadOrEnsureProfile(cur);
    if (hydrated != null && !controller.isClosed) {
      _currentUser = hydrated;
      await SessionService.saveUser(hydrated);
      controller.add(hydrated);
    }
  }

  AppUser _mapUser(User user, {Map<String, dynamic>? profile}) {
    final meta = user.userMetadata ?? {};
    final p = profile ?? const <String, dynamic>{};
    final roleStr = (p['role'] ?? meta['role']) as String? ?? 'officer';
    return AppUser(
      id: user.id,
      email: (p['email'] as String?) ?? user.email ?? '',
      fullName: (p['full_name'] ?? meta['full_name']) as String? ??
          user.email?.split('@').first ??
          'User',
      role: UserRole.fromString(roleStr),
      phone: (p['phone'] ?? meta['phone']) as String? ?? user.phone,
      department: (p['department'] ?? meta['department']) as String?,
      caseNumber: (p['case_number'] ?? meta['case_number']) as String?,
      officerId: (p['officer_id'] ?? meta['officer_id']) as String?,
      trackerId: (p['tracker_id'] ?? meta['tracker_id']) as String?,
      isActive: (p['is_active'] as bool?) ?? true,
      supervisionStartDate:
          (p['supervision_start_date'] ?? meta['supervision_start_date']) as String?,
      supervisionEndDate:
          (p['supervision_end_date'] ?? meta['supervision_end_date']) as String?,
    );
  }

  Future<AppUser?> _loadOrEnsureProfile(User user) async {
    try {
      final rows = await _supabase
          .from('profiles')
          .select()
          .eq('id', user.id)
          .limit(1);
      if ((rows as List).isNotEmpty) {
        return _mapUser(user, profile: Map<String, dynamic>.from(rows.first as Map));
      }
      final mapped = _mapUser(user);
      try {
        await _supabase.from('profiles').upsert({
          'id': mapped.id,
          'email': mapped.email,
          'full_name': mapped.fullName,
          'role': mapped.role.name,
          if (mapped.phone != null) 'phone': mapped.phone,
          if (mapped.caseNumber != null) 'case_number': mapped.caseNumber,
          if (mapped.department != null) 'department': mapped.department,
          if (mapped.officerId != null) 'officer_id': mapped.officerId,
          'is_active': true,
        });
      } catch (_) {}
      return mapped;
    } catch (_) {
      return null;
    }
  }

  @override
  Future<AppUser> signIn({
    required String email,
    required String password,
    required UserRole selectedRole,
  }) async {
    final cleanEmail = email.trim().toLowerCase();

    // Always attempt real Supabase Authentication first so GPS & database RLS have a valid JWT.
    try {
      final res = await _supabase.auth.signInWithPassword(
        email: cleanEmail,
        password: password,
      );

      final user = res.user;
      if (user == null) {
        throw AuthException('No user account returned from Supabase.');
      }

      final appUser = await _loadOrEnsureProfile(user) ?? _mapUser(user);
      if (appUser.role != selectedRole) {
        try {
          await _supabase.auth.signOut();
        } catch (_) {}
        throw AuthException(
          'Role mismatch. This account is registered as a ${appUser.role.label}, but you selected ${selectedRole.label}.',
        );
      }

      _currentUser = appUser;
      await SessionService.saveUser(appUser);
      return appUser;
    } on AuthException {
      rethrow;
    } on AuthApiException catch (e) {
      if (e.message.toLowerCase().contains('email not confirmed')) {
        throw AuthException(
          'Email not confirmed. In Supabase Dashboard → Authentication → Providers → Email, turn OFF "Confirm email" to allow instant login.',
        );
      }
      throw AuthException(e.message);
    } catch (e) {
      // Offline / network fallback for demo accounts only when Supabase cannot be reached.
      if (cleanEmail.contains('ppa-attendance.com') || cleanEmail.contains('ppa-parolee.org')) {
        try {
          final demoUser = await _demoFallback.signIn(
            email: cleanEmail,
            password: password,
            selectedRole: selectedRole,
          );
          _currentUser = demoUser;
          return demoUser;
        } catch (_) {}
      }
      throw AuthException(e.toString());
    }
  }

  @override
  Future<void> register({
    required String email,
    required String password,
    required String fullName,
    required UserRole role,
    String? phone,
  }) async {
    try {
      final res = await _supabase.auth.signUp(
        email: email.trim(),
        password: password,
        data: {
          'full_name': fullName,
          'role': role.name,
          'phone': phone,
        },
      );

      if (res.user != null) {
        try {
          await _supabase.from('profiles').upsert({
            'id': res.user!.id,
            'email': email.trim(),
            'full_name': fullName,
            'role': role.name,
            'phone': phone,
            'created_at': DateTime.now().toIso8601String(),
          });
        } catch (_) {}
      }
    } on AuthApiException catch (e) {
      throw AuthException(e.message);
    } catch (e) {
      throw AuthException('Registration failed: $e');
    }
  }

  @override
  Future<void> resendVerification({
    required String email,
    required String password,
  }) async {
    try {
      await _supabase.auth.resend(
        type: OtpType.signup,
        email: email.trim(),
      );
    } catch (e) {
      throw AuthException('Failed to resend verification: $e');
    }
  }

  @override
  Future<void> signOut() async {
    try {
      await _supabase.auth.signOut();
    } catch (_) {}
    _currentUser = null;
    await SessionService.clear();
  }
}
