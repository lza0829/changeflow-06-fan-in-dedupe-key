# fan_in_dedupe_key：多入口同实体键汇入去重。
#
# 本包为切口占位；可复用实现位于：
# - app.engines.fan_in_rules：固定裁决规则与纯函数（DEDUPE_RULE / dedupe_by_entity_key / gap_decision）
# - app.services.fan_in_service：拉取入口夹具 → 入候选清单 → 同键裁决 → 共享通道 Apply
