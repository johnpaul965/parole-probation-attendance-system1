class AppConfig {
  static const bool useSupabase = true;

  /// GPS simulation is off for production ingest (ESP32 / Supabase realtime).
  static bool get enableMockLocationSimulator => false;
}
