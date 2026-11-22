import 'dart:async';
import 'dart:math' as math;

import 'package:flutter/foundation.dart';
import 'package:last/core/constants/tracking_config.dart';
import 'package:last/core/services/session_service.dart';
import 'package:last/core/utils/app_config.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/tracking/models/location_record.dart';
import 'package:last/features/tracking/services/supabase_tracking_repository.dart';

/// Development-only service that simulates GPS hardware submitting locations
/// into Supabase. Isolated from production — only runs when enabled.
class MockLocationSimulatorService {
  MockLocationSimulatorService({
    SupabaseTrackingRepository? repository,
  }) : _repository = repository ?? SupabaseTrackingRepository();

  final SupabaseTrackingRepository _repository;
  final _random = math.Random(42);
  Timer? _timer;
  int _tick = 0;

  static MockLocationSimulatorService? _instance;

  static void startIfEnabled() {
    if (!AppConfig.enableMockLocationSimulator) return;
    _instance ??= MockLocationSimulatorService();
    _instance!._start();
  }

  static void stop() {
    _instance?._timer?.cancel();
    _instance = null;
  }

  void _start() {
    if (_timer != null) return;
    debugPrint('[MockLocationSimulator] Started — simulating GPS device submissions');
    _timer = Timer.periodic(TrackingConfig.mockUpdateInterval, (_) => _submitAll());
    _submitAll();
  }

  Future<void> _submitAll() async {
    _tick++;
    if (!SupabaseConfig.isConfigured) return;
    final currentUser = SupabaseConfig.client.auth.currentUser ?? SessionService.getUser();
    if (currentUser == null) return;

    try {
      final rows = await SupabaseConfig.client.from('gps_trackers').select();

      for (final r in rows as List) {
        final data = Map<String, dynamic>.from(r as Map);
        final id = data['id']?.toString() ?? '';
        final paroleeId = (data['parolee_id'] ?? data['paroleeId']) as String?;
        final deviceToken = (data['device_token'] ?? data['deviceToken']) as String? ?? '';
        if (id.isEmpty || paroleeId == null) continue;

        // Simulate one offline tracker (TRK-002 pattern) for demo scenarios.
        if (id.endsWith('-002')) continue;

        final angle = _tick * 0.3 + id.hashCode * 0.01;
        final orbit = TrackingConfig.mockProximityOrbitDegrees;
        final offsetLat = math.sin(angle) * orbit;
        final offsetLng = math.cos(angle) * orbit;
        final battery = 75 + (_tick % 10);

        final location = LocationRecord(
          id: '',
          trackerId: id,
          paroleeId: paroleeId,
          latitude: TrackingConfig.mockBaseLatitude + offsetLat,
          longitude: TrackingConfig.mockBaseLongitude + offsetLng,
          accuracy: 4.0 + _random.nextDouble() * 3,
          speed: 2.0 + _random.nextDouble() * 4,
          batteryLevel: battery,
          recordedAt: DateTime.now(),
          source: GpsSource.simulated,
        );

        await _repository.submitLocation(
          trackerId: id,
          deviceToken: deviceToken,
          location: location,
        );
      }
    } catch (e) {
      debugPrint('[MockLocationSimulator] Submission error: $e');
    }
  }

  void dispose() {
    _timer?.cancel();
    _timer = null;
  }
}
