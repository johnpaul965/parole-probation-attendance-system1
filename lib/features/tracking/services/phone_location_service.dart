import 'dart:async';

import 'package:geolocator/geolocator.dart';
import 'package:last/core/constants/tracking_config.dart';
import 'package:last/core/services/session_service.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';

class PhoneLocationException implements Exception {
  PhoneLocationException(this.message);
  final String message;
  @override
  String toString() => message;
}

/// Foreground GPS sharing from the signed-in parolee's phone into Supabase `location_logs`.
class PhoneLocationService {
  PhoneLocationService._();
  static final PhoneLocationService instance = PhoneLocationService._();

  Timer? _timer;
  String? _paroleeId;
  DateTime? lastSentAt;
  String? lastError;
  void Function()? onUpdate;
  Position? _lastPosition;

  bool get isSharing => _timer != null && _paroleeId != null;

  String? _currentUserId() {
    if (SupabaseConfig.isConfigured) {
      final sbUid = SupabaseConfig.client.auth.currentUser?.id;
      if (sbUid != null && sbUid.isNotEmpty) return sbUid;
    }
    return SessionService.getUser()?.id;
  }

  Future<void> start(String paroleeId) async {
    final uid = _currentUserId();
    if (uid == null || uid.isEmpty) {
      throw PhoneLocationException('Sign in as a parolee to share location.');
    }
    if (paroleeId != uid) {
      throw PhoneLocationException('You can only share location for your own account.');
    }
    if (!SupabaseConfig.isConfigured) {
      throw PhoneLocationException('Supabase is not configured.');
    }
    if (SupabaseConfig.client.auth.currentUser == null) {
      throw PhoneLocationException(
        'Sign in with a valid Supabase account to share live location.',
      );
    }

    await _ensurePermission();
    _paroleeId = uid;
    _lastPosition = null;
    lastError = null;
    _timer?.cancel();
    await _publish();
    _timer = Timer.periodic(const Duration(seconds: 45), (_) async {
      try {
        lastError = null;
        await _publish();
      } catch (e, st) {
        lastError = e.toString();
        AppLogger.e('PhoneLocationService', 'Periodic publish failed', e, st);
      }
      onUpdate?.call();
    });
    onUpdate?.call();
    AppLogger.d('PhoneLocationService', 'Sharing started', uid);
  }

  void stop() {
    _timer?.cancel();
    _timer = null;
    _paroleeId = null;
    lastError = null;
    AppLogger.d('PhoneLocationService', 'Sharing stopped');
    onUpdate?.call();
  }

  Future<void> _ensurePermission() async {
    final enabled = await Geolocator.isLocationServiceEnabled();
    if (!enabled) {
      throw PhoneLocationException('Turn on location services, then try again.');
    }

    var permission = await Geolocator.checkPermission();
    if (permission == LocationPermission.denied) {
      permission = await Geolocator.requestPermission();
    }
    if (permission == LocationPermission.denied) {
      throw PhoneLocationException('Location permission was denied.');
    }
    if (permission == LocationPermission.deniedForever) {
      throw PhoneLocationException(
        'Location permission is blocked. Enable it in system settings.',
      );
    }
  }

  Future<Position> _acquirePosition() async {
    try {
      return await Geolocator.getCurrentPosition(
        locationSettings: const LocationSettings(
          accuracy: LocationAccuracy.high,
          timeLimit: Duration(seconds: 15),
        ),
      );
    } on TimeoutException {
      // Fall back to medium (Wi-Fi / network) accuracy for indoor devices & desktops.
      try {
        return await Geolocator.getCurrentPosition(
          locationSettings: const LocationSettings(
            accuracy: LocationAccuracy.medium,
            timeLimit: Duration(seconds: 12),
          ),
        );
      } on TimeoutException {
        try {
          final lastKnown = await Geolocator.getLastKnownPosition();
          if (lastKnown != null) {
            final age = DateTime.now().difference(lastKnown.timestamp);
            if (age <= const Duration(minutes: 5)) {
              return lastKnown;
            }
          }
        } catch (_) {}
        rethrow;
      }
    }
  }

  Future<void> _publish() async {
    final uid = _paroleeId ?? _currentUserId();
    if (uid == null) {
      stop();
      return;
    }

    try {
      final pos = await _acquirePosition();

      if (!pos.latitude.isFinite ||
          !pos.longitude.isFinite ||
          (pos.latitude.abs() < 0.0001 && pos.longitude.abs() < 0.0001)) {
        throw PhoneLocationException('GPS does not have a valid fix yet.');
      }

      final now = DateTime.now();
      if (_lastPosition != null &&
          lastSentAt != null &&
          now.difference(lastSentAt!) < TrackingConfig.duplicateLocationWindow) {
        final d = Geolocator.distanceBetween(
          _lastPosition!.latitude,
          _lastPosition!.longitude,
          pos.latitude,
          pos.longitude,
        );
        if (d < 8) return;
      }

      final accuracy =
          (pos.accuracy.isFinite && pos.accuracy >= 0) ? pos.accuracy : 15.0;
      final speedKmh =
          (pos.speed.isFinite && pos.speed >= 0) ? pos.speed * 3.6 : 0.0;

      // Omit created_at so Postgres DEFAULT now() populates it (column-level grants reject client-supplied created_at).
      await SupabaseConfig.client.from('location_logs').insert({
        'tracker_id': 'MOBILE-GPS',
        'parolee_id': uid,
        'latitude': pos.latitude,
        'longitude': pos.longitude,
        'accuracy': accuracy,
        'speed': speedKmh,
      });

      _lastPosition = pos;
      lastSentAt = now;
      AppLogger.d('PhoneLocationService', 'Location saved', '$uid ${pos.latitude}');
    } on PhoneLocationException {
      rethrow;
    } on TimeoutException {
      throw PhoneLocationException('Timed out waiting for GPS. Try outdoors.');
    } catch (e) {
      throw PhoneLocationException(e.toString().replaceFirst('Exception: ', ''));
    }
  }
}
