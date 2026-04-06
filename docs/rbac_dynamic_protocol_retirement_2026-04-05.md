# RBAC 动态协议退役计划（2026-04-05）

## 背景

当前系统已切换到动态 RBAC 绑定主路径：

- 导航：`/api/navigation-groups`
- 路由权限与权限目录：`/api/auth/permissions` 增量字段
- 登录状态：`/api/auth/status` 增量字段（动态版本与来源）

为保证一次切换后的回退能力，旧字段暂时保留一个发布周期。

## 新增字段

### `/api/auth/permissions`

- `permission_catalog`
- `route_permissions`
- `navigation_groups`
- `schema_version`
- `version`
- `source`
- `rbac_dynamic_enforced`
- `validation`

### `/api/auth/status`

- `rbac_dynamic_enforced`
- `rbac_dynamic_version`
- `rbac_dynamic_source`
- `rbac_schema_version`
- `dynamic_rbac`

## 兼容字段（暂保留）

- `permission_matrix`
- `effective_permissions`
- 其余历史认证状态字段

## 退役时间窗

- 兼容期：`2026-04-05` 至 `2026-05-05`
- 目标退役日：`2026-05-06`

## 下线前置条件

1. 动态配置一致性脚本连续 7 天通过：
   - `python3 scripts/check_rbac_dynamic_config.py`
2. 权限 smoke 连续 7 天通过：
   - `python3 scripts/smoke_navigation_permissions.py`
3. 无新增越权/误拦截告警
4. 前端生产版本全部使用动态路由绑定

## 回滚策略

- 紧急回滚：将 `RBAC_DYNAMIC_ENFORCED` 设为 `false`，回退到旧判定路径。
- 协议回滚：保留旧字段期间可直接回退前端版本。
