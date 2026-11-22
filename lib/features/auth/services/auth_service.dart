import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/auth/models/app_user.dart';
import 'package:last/features/auth/models/user_role.dart';
import 'package:last/features/auth/services/demo_auth_service.dart';
import 'package:last/features/auth/services/supabase_auth_service.dart';

class AuthException implements Exception {
  AuthException(this.message);
  final String message;
  @override
  String toString() => message;
}

abstract class AuthService {
  Future<AppUser> signIn({
    required String email,
    required String password,
    required UserRole selectedRole,
  });

  Future<void> register({
    required String email,
    required String password,
    required String fullName,
    required UserRole role,
    String? phone,
  });

  Future<void> resendVerification({
    required String email,
    required String password,
  });

  Future<void> signOut();
  AppUser? get currentUser;
  Stream<AppUser?> get authStateChanges;
}

AuthService createAuthService() {
  if (SupabaseConfig.isConfigured) {
    return SupabaseAuthService();
  }
  return DemoAuthService();
}
