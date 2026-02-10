# Failure cases (realistic)

1. **False drift alarm**
   - A marketing event changes traffic temporarily.
   - Detectors fire; adaptation freezes; alert rate spikes.
   - Mitigation: severity gating, human review, widen thresholds, use calendar-aware suppressions.

2. **Silent drift**
   - Distribution shifts slowly; KS/PSI thresholds not triggered.
   - Model quality degrades; anomalies become normal.
   - Mitigation: periodic evaluation, additional detectors, scheduled reference review.

3. **Adaptation hides incidents**
   - Threshold adapts upward due to gradual score changes.
   - Real anomalies get suppressed.
   - Mitigation: max step, cooldown, freeze-on-drift, audit logs, manual approval.

4. **State blow-up**
   - Entity cardinality spikes; window store memory grows.
   - Mitigation: max_events_per_entity, eviction, drop policies, backpressure.
