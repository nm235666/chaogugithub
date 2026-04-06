# 导航与权限轻量收口验收清单（2026-04-05）

## 1. 构建与静态检查

- `cd apps/web && npm run build`
- `python3 -m py_compile backend/server.py backend/routes/system.py`

## 2. 配置一致性校验

- `python3 scripts/check_navigation_config_alignment.py`
- `python3 scripts/check_rbac_dynamic_config.py`
- 预期：
  - 输出 `[OK] navigation groups aligned`
  - 输出 `[OK] rbac_dynamic config consistency passed`
  - frontend/backend version 可读

## 3. 接口与权限 smoke

- `python3 scripts/smoke_navigation_permissions.py`
- 预期：
  - `/api/navigation-groups` 返回组结构完整
  - admin/pro/limited 在权限过滤后都至少有可见分组

## 4. 页面手工 smoke（建议）

- `/system/permissions`
  - 显示“分组与权限码映射”
  - 显示“映射版本”
  - 保存时越权权限不会被写回
- 任意页面导航
  - 导航分组无空组
  - 不具备权限的菜单不显示
  - 远端导航异常时仍使用本地配置可访问页面

## 5. 回归关注

- 路由守卫行为不变（无权限仍跳转升级/登录链路）
- 角色策略更新接口语义不变（`permissions` 数组提交）
- 若启用全动态强制模式，确认 `RBAC_DYNAMIC_ENFORCED=true` 时缺映射路由会被拒绝访问
