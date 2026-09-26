package dev.engnotes.fes.riskalert.rules;

/** The kinds of per-trade state a rule can read. One value per store the engine can apply. */
public enum StateKind {
    POSITION,
    VOLUME_WINDOW,
    RECENT_TRADES
}
