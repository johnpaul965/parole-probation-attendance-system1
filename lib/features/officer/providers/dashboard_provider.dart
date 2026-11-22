import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:last/core/services/live_stats_service.dart';
import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/officer/models/dashboard_stats.dart';
import 'package:last/features/officer/models/live_stats.dart';
import 'package:last/features/officer/models/parolee_summary.dart';
import 'package:last/features/officer/services/officer_service.dart';
import 'package:last/features/shared/models/attendance_record.dart';
import 'package:last/features/shared/models/schedule_model.dart';

class DashboardProvider extends ChangeNotifier {
  DashboardProvider({
    OfficerService? service,
    LiveStatsService? liveStatsService,
  })  : _service = service ?? createOfficerService(),
        _liveStatsService = liveStatsService ?? LiveStatsService();

  final OfficerService _service;
  final LiveStatsService _liveStatsService;
  DashboardStats? _stats;
  LiveStats? _liveStats;
  List<ParoleeSummary> _parolees = [];
  List<ScheduleModel> _schedules = [];
  bool _loading = false;
  String? _error;
  String _search = '';
  AttendanceStatus? _filter;
  String? _officerId;
  StreamSubscription<DashboardSnapshot>? _sub;
  StreamSubscription<LiveStats?>? _liveSub;

  DashboardStats? get stats => _liveStats?.stats ?? _stats;
  LiveStats? get liveStats => _liveStats;
  List<ParoleeSummary> get parolees => _parolees;
  List<ParoleeSummary> get checkedInList =>
      _liveStats?.checkedIn ??
      _parolees
          .where((p) =>
              p.status == AttendanceStatus.checkedIn ||
              p.status == AttendanceStatus.confirmed)
          .toList();
  List<ParoleeSummary> get pendingList =>
      _liveStats?.pending ?? _parolees.where((p) => p.status == AttendanceStatus.pending).toList();
  List<ParoleeSummary> get missedList =>
      _liveStats?.missed ?? _parolees.where((p) => p.status == AttendanceStatus.missed).toList();
  List<ScheduleModel> get schedules => _schedules;
  bool get isLoading => _loading;
  String? get error => _error;
  DateTime? get lastUpdated => _liveStats?.updatedAt;

  void startListening(String officerId) {
    if (officerId.isEmpty) {
      _error = 'Officer account ID is missing. Please sign out and sign in again.';
      _loading = false;
      notifyListeners();
      return;
    }

    if (!SupabaseConfig.isConfigured) {
      _error = 'Supabase is not configured. Check SupabaseConfig or use demo credentials offline.';
      _loading = false;
      _stats = const DashboardStats(totalCases: 0, checkedIn: 0, pending: 0, missed: 0);
      _parolees = [];
      notifyListeners();
      return;
    }

    _officerId = officerId;
    _sub?.cancel();
    _liveSub?.cancel();
    _loading = true;
    _error = null;
    notifyListeners();

    AppLogger.d('DashboardProvider', 'startListening', officerId);

    _liveSub = _liveStatsService.watchOfficer(officerId).listen(
      (live) {
        if (live != null) {
          _liveStats = live;
          _stats = live.stats;
          notifyListeners();
        }
      },
      onError: (Object e, StackTrace st) {
        AppLogger.e('DashboardProvider', 'live_stats stream error', e, st);
      },
    );

    _sub = _service
        .watchDashboard(
          officerId,
          query: _search.isEmpty ? null : _search,
          filter: _filter,
        )
        .listen(
      (snapshot) {
        AppLogger.d(
          'DashboardProvider',
          'Dashboard loaded',
          '${snapshot.parolees.length} parolees',
        );
        _stats = snapshot.stats;
        _parolees = snapshot.parolees;
        _schedules = snapshot.schedules;
        _liveStats = LiveStats.fromDashboard(snapshot.parolees);
        _loading = false;
        _error = null;
        notifyListeners();
      },
      onError: (Object e, StackTrace st) {
        AppLogger.e('DashboardProvider', 'Dashboard stream error', e, st);
        _loading = false;
        if (_parolees.isEmpty) {
          _error = _friendlyError(e);
        } else {
          _error = '${_friendlyError(e)}\nShowing last loaded data.';
        }
        notifyListeners();
      },
    );
  }

  String _friendlyError(Object e) {
    final raw = e.toString().replaceFirst('Exception: ', '');
    if (raw.contains('permission') || raw.contains('42501')) {
      return 'Supabase access denied.\nCheck RLS policies for profiles/attendance_records.';
    }
    if (raw.contains('SocketException') || raw.contains('network')) {
      return 'Cannot reach Supabase. Check internet connection.';
    }
    return raw.isNotEmpty ? raw : 'Failed to load live dashboard';
  }

  Future<void> refresh(String officerId) async {
    startListening(officerId);
  }

  void setSearch(String value) {
    _search = value;
    if (_officerId != null) startListening(_officerId!);
  }

  void setFilter(AttendanceStatus? status) {
    _filter = status;
    if (_officerId != null) startListening(_officerId!);
  }

  bool _disposed = false;

  void stop({bool notify = false}) {
    _sub?.cancel();
    _sub = null;
    _liveSub?.cancel();
    _liveSub = null;
    _officerId = null;
    _stats = null;
    _liveStats = null;
    _parolees = [];
    _schedules = [];
    _loading = false;
    _error = null;
    if (notify && !_disposed) {
      notifyListeners();
    }
  }

  @override
  void dispose() {
    _disposed = true;
    _sub?.cancel();
    _liveSub?.cancel();
    super.dispose();
  }
}
