import 'dart:async';

import 'package:last/core/utils/app_logger.dart';
import 'package:last/core/utils/supabase_config.dart';
import 'package:last/features/shared/models/notification_model.dart';
import 'package:uuid/uuid.dart';

abstract class NotificationService {
  Stream<List<NotificationModel>> watchForUser(String userId);
  Future<List<NotificationModel>> getForUser(String userId);
  Future<void> markRead(String id);
  Future<void> send({
    required String userId,
    required String title,
    required String body,
    required NotificationType type,
  });
  Future<void> initializeMessaging();
}

class SupabaseNotificationService implements NotificationService {
  final InMemoryNotificationService _fallback = InMemoryNotificationService.instance;

  @override
  Stream<List<NotificationModel>> watchForUser(String userId) {
    if (!SupabaseConfig.isConfigured || userId.isEmpty) {
      return _fallback.watchForUser(userId);
    }
    try {
      return SupabaseConfig.client
          .from('notifications')
          .stream(primaryKey: ['id'])
          .eq('user_id', userId)
          .map((rows) {
            final list = rows.map(_mapRow).toList()
              ..sort((a, b) => b.createdAt.compareTo(a.createdAt));
            return list;
          })
          .handleError((Object e) {
            AppLogger.e('NotificationService', 'watchForUser stream note', e);
          });
    } catch (e) {
      AppLogger.e('NotificationService', 'watchForUser setup note', e);
      return _fallback.watchForUser(userId);
    }
  }

  @override
  Future<List<NotificationModel>> getForUser(String userId) async {
    if (!SupabaseConfig.isConfigured || userId.isEmpty) {
      return _fallback.getForUser(userId);
    }
    try {
      final rows = await SupabaseConfig.client
          .from('notifications')
          .select()
          .eq('user_id', userId)
          .order('created_at', ascending: false);
      return (rows as List).map((r) => _mapRow(Map<String, dynamic>.from(r as Map))).toList();
    } catch (e) {
      AppLogger.e('NotificationService', 'getForUser note', e);
      return _fallback.getForUser(userId);
    }
  }

  @override
  Future<void> markRead(String id) async {
    await _fallback.markRead(id);
    if (!SupabaseConfig.isConfigured) return;
    try {
      await SupabaseConfig.client.from('notifications').update({'read': true}).eq('id', id);
    } catch (e) {
      AppLogger.e('NotificationService', 'markRead note', e);
    }
  }

  @override
  Future<void> send({
    required String userId,
    required String title,
    required String body,
    required NotificationType type,
  }) async {
    if (userId.isEmpty) return;
    await _fallback.send(userId: userId, title: title, body: body, type: type);
    if (!SupabaseConfig.isConfigured) return;
    try {
      await SupabaseConfig.client.from('notifications').insert({
        'id': const Uuid().v4(),
        'user_id': userId,
        'title': title,
        'body': body,
        'type': type.name,
        'created_at': DateTime.now().toIso8601String(),
        'read': false,
      });
    } catch (e) {
      AppLogger.e('NotificationService', 'send note', e);
    }
  }

  @override
  Future<void> initializeMessaging() async {}

  NotificationModel _mapRow(Map<String, dynamic> r) {
    return NotificationModel.fromMap({
      'id': r['id']?.toString() ?? '',
      'userId': r['user_id'] ?? r['userId'] ?? '',
      'title': r['title'] ?? '',
      'body': r['body'] ?? '',
      'type': r['type'] ?? 'info',
      'createdAt': r['created_at'] ?? r['createdAt'] ?? DateTime.now().toIso8601String(),
      'read': r['read'] ?? false,
    });
  }
}

/// In-app notification list for demo / offline fallback.
class InMemoryNotificationService implements NotificationService {
  InMemoryNotificationService();

  static final InMemoryNotificationService instance = InMemoryNotificationService();

  final List<NotificationModel> _items = [];
  final _controller = StreamController<List<NotificationModel>>.broadcast();

  @override
  Stream<List<NotificationModel>> watchForUser(String userId) {
    Future.microtask(() => _emit(userId));
    return _controller.stream.map((all) => all.where((n) => n.userId == userId).toList());
  }

  @override
  Future<List<NotificationModel>> getForUser(String userId) async {
    return _items.where((n) => n.userId == userId).toList();
  }

  @override
  Future<void> markRead(String id) async {
    final idx = _items.indexWhere((n) => n.id == id);
    if (idx != -1) {
      _items[idx] = _items[idx].copyWith(read: true);
      _controller.add(List.unmodifiable(_items));
    }
  }

  @override
  Future<void> send({
    required String userId,
    required String title,
    required String body,
    required NotificationType type,
  }) async {
    _items.insert(
      0,
      NotificationModel(
        id: const Uuid().v4(),
        userId: userId,
        title: title,
        body: body,
        type: type,
        createdAt: DateTime.now(),
      ),
    );
    _controller.add(List.unmodifiable(_items));
  }

  @override
  Future<void> initializeMessaging() async {}

  void _emit(String userId) {
    _controller.add(_items.where((n) => n.userId == userId).toList());
  }
}

NotificationService createNotificationService() {
  if (SupabaseConfig.isConfigured) {
    return SupabaseNotificationService();
  }
  return InMemoryNotificationService.instance;
}
