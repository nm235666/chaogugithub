# 导航重构与权限系统改造方案

> **文档版本**: 1.1  
> **创建日期**: 2026-04-05  
> **文档状态**: 分阶段执行中（阶段一完成 + 阶段二轻量版完成）  

## 执行进展（2026-04-05）

- 已完成：
  - 导航重构为 7 大分组，保持原权限码语义。
  - 角色权限页改为分组视图，保存逻辑仍按现有 `permissions` 数组提交。
  - 新增 `/api/navigation-groups`，前端支持远端优先 + 本地兜底。
  - 增加导航配置结构校验与可执行 smoke 脚本。
- 本轮未做：
  - 新权限码（如 `stocks_read/signals_read`）切换。
  - 权限组数据库建模与迁移。
  - 新角色类型扩展。

---

## 目录

1. [现状分析](#一现状分析)
2. [目标导航分类方案](#二目标导航分类方案)
3. [权限系统分析](#三权限系统分析)
4. [前端改造方案](#四前端改造方案)
5. [后端改造方案](#五后端改造方案)
6. [实施路线图](#六实施路线图)
7. [附录](#七附录)

---

## 一、现状分析

### 1.1 当前导航结构

```
📊 AppShell.vue 当前分组（4大类）
├── 📁 总控
│   ├── 总控台
│   ├── 数据源监控
│   ├── 任务调度中心
│   ├── LLM节点管理
│   ├── 角色权限策略
│   ├── 数据库审计
│   ├── 邀请码管理
│   └── 用户与会话
├── 📁 股票
│   ├── 股票列表
│   ├── 综合评分
│   ├── 股票详情
│   └── 价格中心
├── 📁 情报与信号
│   ├── 国际资讯
│   ├── 国内资讯
│   ├── 个股新闻
│   ├── 新闻日报总结
│   ├── 投资信号
│   ├── 主题热点
│   ├── 信号质量审计
│   ├── 信号质量配置
│   └── 状态时间线
└── 📁 研究与舆情
    ├── 宏观看板
    ├── 走势分析
    ├── 标准报告
    ├── 因子挖掘
    ├── 多角色分析
    ├── 群聊总览
    ├── 聊天记录
    ├── 投资倾向
    └── 股票候选池
```

### 1.2 当前权限系统结构

**权限码定义**（`apps/web/src/app/permissions.ts`）：

```typescript
export type AppPermission =
  | 'public'           // 公开访问
  | 'news_read'        // 国际/国内资讯阅读
  | 'stock_news_read'  // 个股新闻阅读
  | 'daily_summary_read'  // 新闻日报总结
  | 'trend_analyze'    // 走势分析（LLM）
  | 'multi_role_analyze'  // 多角色分析（LLM）
  | 'admin_users'      // 用户管理
  | 'admin_system'     // 系统管理
  | 'research_advanced'   // 高级研究
  | 'signals_advanced'    // 信号高级功能
  | 'chatrooms_advanced'  // 群聊高级功能
  | 'stocks_advanced'     // 股票数据高级
  | 'macro_advanced'      // 宏观数据高级
```

**角色定义**（`backend/server.py`）：

| 角色 | 权限范围 | 日配额限制 |
|------|----------|------------|
| **admin** | 全部权限 (`*`) | 无限制 |
| **pro** | 除 admin 外的全部权限 | 走势分析 200 次/天，多角色分析 80 次/天 |
| **limited** | 仅基础阅读和分析权限 | 走势分析 30 次/天，多角色分析 10 次/天 |

### 1.3 当前系统问题

| 问题 | 说明 | 影响 |
|------|------|------|
| 导航层次不清晰 | 4大类但功能混合 | 新用户难以定位功能 |
| 权限粒度粗 | `*_advanced` 包含过多功能 | 无法精细化控制 |
| 权限与导航不匹配 | 导航分组 ≠ 权限分组 | 维护困难 |
| 角色数量少 | 只有 3 个角色 | 难以满足复杂场景 |
| 前端硬编码 | 权限定义分散在多个文件 | 容易不一致 |

---

## 二、目标导航分类方案

### 2.1 推荐方案：角色导向分类（7大类）

```
🎯 新导航结构
│
├── 📊 工作台
│   ├── 总控台（管理员）
│   └── 我的关注（所有用户）
│
├── 📈 市场数据
│   ├── 股票列表
│   ├── 综合评分
│   ├── 股票详情
│   └── 价格中心
│
├── 📰 资讯中心
│   ├── 国际资讯
│   ├── 国内资讯
│   ├── 个股新闻
│   └── 新闻日报总结
│
├── 📡 信号研究
│   ├── 投资信号
│   ├── 主题热点
│   ├── 信号质量审计
│   ├── 信号质量配置
│   └── 状态时间线
│
├── 🔬 深度研究
│   ├── 宏观看板
│   ├── 走势分析
│   ├── 标准报告
│   ├── 因子挖掘
│   └── 多角色分析
│
├── 💬 舆情监控
│   ├── 群聊总览
│   ├── 聊天记录
│   ├── 投资倾向
│   └── 股票候选池
│
└── ⚙️ 系统管理
    ├── 数据源监控
    ├── 任务调度中心
    ├── LLM节点管理
    ├── 角色权限策略
    ├── 数据库审计
    ├── 邀请码管理
    └── 用户与会话
```

### 2.2 分类说明

| 分类 | 目标用户 | 功能定位 |
|------|----------|----------|
| 工作台 | 所有用户 | 入口页面，个性化关注 |
| 市场数据 | 研究员/交易员 | 行情数据查询 |
| 资讯中心 | 所有用户 | 新闻阅读 |
| 信号研究 | 量化研究员 | 信号生成与审计 |
| 深度研究 | 分析师 | LLM 辅助分析工具 |
| 舆情监控 | 风控/研究员 | 群聊数据监控 |
| 系统管理 | 管理员 | 平台运维管理 |

---

## 三、权限系统分析

### 3.1 当前权限-导航映射表

| 导航项 | 当前所需权限 | 所属分类 |
|--------|-------------|----------|
| 总控台 | `admin_system` | 工作台 |
| 数据源监控 | `admin_system` | 系统管理 |
| 任务调度中心 | `admin_system` | 系统管理 |
| LLM节点管理 | `admin_system` | 系统管理 |
| 角色权限策略 | `admin_system` | 系统管理 |
| 数据库审计 | `admin_system` | 系统管理 |
| 邀请码管理 | `admin_users` | 系统管理 |
| 用户与会话 | `admin_users` | 系统管理 |
| 股票列表 | `stocks_advanced` | 市场数据 |
| 综合评分 | `stocks_advanced` | 市场数据 |
| 股票详情 | `stocks_advanced` | 市场数据 |
| 价格中心 | `stocks_advanced` | 市场数据 |
| 国际资讯 | `news_read` | 资讯中心 |
| 国内资讯 | `news_read` | 资讯中心 |
| 个股新闻 | `stock_news_read` | 资讯中心 |
| 新闻日报总结 | `daily_summary_read` | 资讯中心 |
| 投资信号 | `signals_advanced` | 信号研究 |
| 主题热点 | `signals_advanced` | 信号研究 |
| 信号质量审计 | `signals_advanced` | 信号研究 |
| 信号质量配置 | `signals_advanced` | 信号研究 |
| 状态时间线 | `signals_advanced` | 信号研究 |
| 宏观看板 | `macro_advanced` | 深度研究 |
| 走势分析 | `trend_analyze` | 深度研究 |
| 标准报告 | `research_advanced` | 深度研究 |
| 因子挖掘 | `research_advanced` | 深度研究 |
| 多角色分析 | `multi_role_analyze` | 深度研究 |
| 群聊总览 | `chatrooms_advanced` | 舆情监控 |
| 聊天记录 | `chatrooms_advanced` | 舆情监控 |
| 投资倾向 | `chatrooms_advanced` | 舆情监控 |
| 股票候选池 | `chatrooms_advanced` | 舆情监控 |

### 3.2 权限粒度问题分析

```
当前问题示例：stocks_advanced
├── 股票列表（只读）
├── 综合评分（只读）
├── 股票详情（只读）
└── 价格中心（只读）

理想拆分：
├── stocks_read（所有股票页面只读）
└── stocks_config（股票配置管理）
```

### 3.3 建议的权限分组

| 权限组 | 包含权限 | 对应导航分类 |
|--------|----------|--------------|
| **工作台** | `dashboard_read` + `admin_system` | 工作台 |
| **市场数据** | `stocks_read` | 市场数据 |
| **资讯中心** | `news_read` + `stock_news_read` + `daily_summary_read` | 资讯中心 |
| **信号研究** | `signals_read` + `signals_config` | 信号研究 |
| **深度研究** | `research_read` + `trend_analyze` + `multi_role_analyze` | 深度研究 |
| **舆情监控** | `chatrooms_read` + `chatrooms_config` | 舆情监控 |
| **系统管理** | `admin_users` + `admin_system` | 系统管理 |

### 3.4 建议的角色扩展

| 角色 | 权限组合 | 适用人群 |
|------|----------|----------|
| **数据分析师** | 市场数据 + 资讯中心 + 宏观看板 | 宏观研究员 |
| **策略研究员** | 市场数据 + 资讯中心 + 信号研究 + 深度研究 | 量化研究员 |
| **舆情监控员** | 资讯中心 + 舆情监控（只读） | 风控/合规 |
| **高级分析师** | 除系统管理外的全部权限 | 资深研究员 |
| **管理员** | 全部权限 | 系统管理员 |

---

## 四、前端改造方案

### 4.1 改造范围

#### 4.1.1 AppShell.vue 导航重构

```typescript
// 新导航结构定义
interface NavGroup {
  id: string;
  label: string;
  icon: string;
  order: number;
  items: NavItem[];
}

const navGroups: NavGroup[] = [
  {
    id: 'dashboard',
    label: '工作台',
    icon: 'Dashboard',
    order: 1,
    items: [
      { path: '/', label: '总控台', permission: 'dashboard_read' },
      { path: '/watchlist', label: '我的关注', permission: 'public' },
    ]
  },
  {
    id: 'market',
    label: '市场数据',
    icon: 'Chart',
    order: 2,
    items: [
      { path: '/stocks', label: '股票列表', permission: 'stocks_read' },
      { path: '/scores', label: '综合评分', permission: 'stocks_read' },
      { path: '/prices', label: '价格中心', permission: 'stocks_read' },
    ]
  },
  // ... 其他分组
];
```

#### 4.1.2 permissions.ts 重构

```typescript
// 建议的新权限结构
export type PermissionGroup = 
  | 'dashboard'
  | 'market'
  | 'news'
  | 'signals'
  | 'research'
  | 'chatrooms'
  | 'admin';

export type AppPermission =
  // 基础权限
  | 'public'
  | 'dashboard_read'
  
  // 市场数据
  | 'stocks_read'
  | 'stocks_config'
  
  // 资讯
  | 'news_read'
  | 'stock_news_read'
  | 'daily_summary_read'
  
  // 信号研究
  | 'signals_read'
  | 'signals_config'
  
  // 深度研究
  | 'research_read'
  | 'research_config'
  | 'trend_analyze'
  | 'multi_role_analyze'
  
  // 舆情监控
  | 'chatrooms_read'
  | 'chatrooms_config'
  
  // 系统管理
  | 'admin_users'
  | 'admin_system';

// 权限分组映射
export const PERMISSION_GROUPS: Record<PermissionGroup, AppPermission[]> = {
  dashboard: ['dashboard_read'],
  market: ['stocks_read', 'stocks_config'],
  news: ['news_read', 'stock_news_read', 'daily_summary_read'],
  signals: ['signals_read', 'signals_config'],
  research: ['research_read', 'research_config', 'trend_analyze', 'multi_role_analyze'],
  chatrooms: ['chatrooms_read', 'chatrooms_config'],
  admin: ['admin_users', 'admin_system'],
};
```

#### 4.1.3 RolePoliciesPage.vue 更新

```typescript
// 权限分组展示
const permissionGroups = [
  {
    id: 'dashboard',
    label: '工作台',
    permissions: ['dashboard_read'],
  },
  {
    id: 'market',
    label: '市场数据',
    permissions: ['stocks_read', 'stocks_config'],
  },
  // ...
];
```

### 4.2 前端改造清单

| 文件 | 改造内容 | 影响范围 |
|------|----------|----------|
| `AppShell.vue` | 重构导航结构，7大分类 | 全局导航 |
| `permissions.ts` | 新增权限分组概念 | 所有权限检查 |
| `RolePoliciesPage.vue` | 按权限组展示配置 | 权限管理页面 |
| `router/index.ts` | 更新路由 meta 权限 | 路由守卫 |
| 各页面组件 | 更新权限检查逻辑 | 页面访问控制 |

---

## 五、后端改造方案

### 5.1 数据模型变更

#### 5.1.1 新增权限组表

```sql
-- 权限组定义表
CREATE TABLE permission_groups (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    label VARCHAR(100) NOT NULL,
    description TEXT,
    order_index INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 权限组成员表
CREATE TABLE permission_group_members (
    id SERIAL PRIMARY KEY,
    group_code VARCHAR(50) REFERENCES permission_groups(code),
    permission_code VARCHAR(50) NOT NULL,
    UNIQUE(group_code, permission_code)
);

-- 角色权限组关联（替代原有的直接权限关联）
CREATE TABLE role_permission_groups (
    id SERIAL PRIMARY KEY,
    role VARCHAR(20) NOT NULL,
    group_code VARCHAR(50) REFERENCES permission_groups(code),
    permissions JSONB, -- 可选：细粒度控制组内权限
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(role, group_code)
);
```

#### 5.1.2 权限数据迁移

```python
# server.py 中的权限组定义
PERMISSION_GROUPS = {
    "dashboard": {
        "label": "工作台",
        "permissions": ["dashboard_read", "public"],
        "icon": "Dashboard",
    },
    "market": {
        "label": "市场数据",
        "permissions": ["stocks_read", "stocks_config"],
        "icon": "Chart",
    },
    "news": {
        "label": "资讯中心",
        "permissions": ["news_read", "stock_news_read", "daily_summary_read"],
        "icon": "Newspaper",
    },
    "signals": {
        "label": "信号研究",
        "permissions": ["signals_read", "signals_config"],
        "icon": "Signal",
    },
    "research": {
        "label": "深度研究",
        "permissions": ["research_read", "research_config", "trend_analyze", "multi_role_analyze"],
        "icon": "Microscope",
    },
    "chatrooms": {
        "label": "舆情监控",
        "permissions": ["chatrooms_read", "chatrooms_config"],
        "icon": "MessageCircle",
    },
    "admin": {
        "label": "系统管理",
        "permissions": ["admin_users", "admin_system"],
        "icon": "Settings",
    },
}
```

### 5.2 API 接口调整

#### 5.2.1 新增接口

```python
# 获取权限组列表
@app.get("/api/permission-groups")
async def get_permission_groups():
    """返回权限组定义（用于前端导航）"""
    return {
        "groups": [
            {
                "code": code,
                "label": config["label"],
                "icon": config["icon"],
                "permissions": config["permissions"],
            }
            for code, config in PERMISSION_GROUPS.items()
        ]
    }

# 获取角色的权限组配置
@app.get("/api/roles/{role}/permission-groups")
async def get_role_permission_groups(role: str):
    """返回角色拥有的权限组"""
    # 查询数据库中该角色的权限组配置
    # 如果没有则返回默认值
```

#### 5.2.2 现有接口兼容

```python
# 保持现有权限校验接口兼容
@app.get("/api/user/permissions")
async def get_user_permissions(user: User = Depends(get_current_user)):
    """
    返回用户权限（兼容旧格式 + 新增权限组格式）
    {
        "permissions": ["news_read", "stock_news_read", ...],  // 旧格式
        "permission_groups": ["news", "market", ...],  // 新格式
        "role": "pro"
    }
    """
```

### 5.3 后端改造清单

| 模块 | 改造内容 | 优先级 |
|------|----------|--------|
| **数据库** | 新增权限组相关表 | 🔴 高 |
| **数据迁移** | 现有角色权限映射到新结构 | 🔴 高 |
| **server.py** | 定义 PERMISSION_GROUPS | 🟡 中 |
| **API 接口** | 新增权限组相关接口 | 🟡 中 |
| **权限校验** | 保持现有校验逻辑（兼容） | 🟢 低 |
| **角色策略** | 支持按权限组配置角色 | 🟡 中 |

### 5.4 最小化改造方案（推荐第一阶段）

如果希望改动最小，仅做前端导航调整：

```python
# server.py 最小改动 - 仅添加权限组映射
# 不修改数据库，仅增加一个常量定义供前端使用

NAVIGATION_PERMISSION_GROUPS = {
    "dashboard": ["admin_system"],  # 使用现有权限码
    "market": ["stocks_advanced"],
    "news": ["news_read", "stock_news_read", "daily_summary_read"],
    "signals": ["signals_advanced"],
    "research": ["research_advanced", "trend_analyze", "multi_role_analyze", "macro_advanced"],
    "chatrooms": ["chatrooms_advanced"],
    "admin": ["admin_system", "admin_users"],
}

@app.get("/api/navigation-groups")
async def get_navigation_groups():
    """返回导航分组定义（供前端 AppShell 使用）"""
    return {"groups": NAVIGATION_PERMISSION_GROUPS}
```

---

## 六、实施路线图

### 6.1 阶段一：前端导航重构（1-2 天）

**目标**：仅调整前端导航展示，不动权限系统

```
Day 1
├── 更新 AppShell.vue 导航结构
├── 调整导航项顺序和分组
└── 保持现有权限检查逻辑

Day 2
├── 验证各角色用户看到正确的导航
├── 测试移动端响应式
└── 收集用户反馈
```

**交付物**：
- 新导航结构的 AppShell.vue
- 7大分类的导航展示
- 现有权限逻辑完全兼容

### 6.2 阶段二：权限系统优化（3-5 天）

**目标**：细化权限粒度，支持权限组

```
Day 1-2: 数据库变更
├── 创建 permission_groups 表
├── 创建 permission_group_members 表
└── 创建 role_permission_groups 表

Day 3: 数据迁移
├── 现有角色数据迁移
├── 验证权限一致性
└── 回滚方案准备

Day 4-5: 后端接口
├── 新增权限组 API
├── 更新角色策略 API
└── 集成测试
```

**交付物**：
- 权限组数据模型
- 后端 API 更新
- 数据迁移脚本

### 6.3 阶段三：角色扩展（2-3 天）

**目标**：支持更多角色类型

```
Day 1: 角色定义
├── 定义新角色权限组合
├── 配置角色日配额
└── 更新角色策略 UI

Day 2-3: 测试验证
├── 各角色权限验证
├── 边界情况测试
└── 性能测试
```

**交付物**：
- 5+ 角色类型支持
- 角色权限配置 UI
- 角色切换测试

### 6.4 风险与回滚

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 权限数据迁移错误 | 用户无法访问功能 | 保留原表，支持快速回滚 |
| 前端导航显示异常 | 用户体验下降 | 保留旧导航代码分支 |
| 缓存不一致 | 权限显示错误 | 改造期间禁用权限缓存 |
| 第三方集成影响 | 外部系统调用失败 | 保持 API 兼容性 |

---

## 七、附录

### 7.1 当前权限-角色映射表

```
┌─────────────────────┬─────────┬─────────┬─────────┐
│      权限码          │  admin  │   pro   │ limited │
├─────────────────────┼─────────┼─────────┼─────────┤
│ * (通配符)          │    ✓    │    ✗    │    ✗    │
│ public              │    ✓    │    ✓    │    ✓    │
│ news_read           │    ✓    │    ✓    │    ✓    │
│ stock_news_read     │    ✓    │    ✓    │    ✓    │
│ daily_summary_read  │    ✓    │    ✓    │    ✓    │
│ trend_analyze       │    ✓    │    ✓    │    ✓    │
│ multi_role_analyze  │    ✓    │    ✓    │    ✓    │
│ research_advanced   │    ✓    │    ✓    │    ✗    │
│ signals_advanced    │    ✓    │    ✓    │    ✗    │
│ chatrooms_advanced  │    ✓    │    ✓    │    ✗    │
│ stocks_advanced     │    ✓    │    ✓    │    ✗    │
│ macro_advanced      │    ✓    │    ✓    │    ✗    │
│ admin_users         │    ✓    │    ✗    │    ✗    │
│ admin_system        │    ✓    │    ✗    │    ✗    │
├─────────────────────┼─────────┼─────────┼─────────┤
│ 走势分析日配额       │   无限  │   200   │   30    │
│ 多角色分析日配额     │   无限  │   80    │   10    │
└─────────────────────┴─────────┴─────────┴─────────┘
```

### 7.2 改造后权限-角色映射（建议）

```
┌─────────────────────┬─────────┬─────────┬─────────┬────────────┐
│      权限码          │  admin  │  pro    │ limited │  data_analyst │
├─────────────────────┼─────────┼─────────┼─────────┼────────────┤
│ dashboard_read      │    ✓    │    ✓    │    ✓    │      ✓     │
│ stocks_read         │    ✓    │    ✓    │    ✓    │      ✓     │
│ stocks_config       │    ✓    │    ✓    │    ✗    │      ✗     │
│ news_read           │    ✓    │    ✓    │    ✓    │      ✓     │
│ stock_news_read     │    ✓    │    ✓    │    ✓    │      ✓     │
│ daily_summary_read  │    ✓    │    ✓    │    ✓    │      ✓     │
│ signals_read        │    ✓    │    ✓    │    ✗    │      ✗     │
│ signals_config      │    ✓    │    ✓    │    ✗    │      ✗     │
│ research_read       │    ✓    │    ✓    │    ✗    │      ✗     │
│ research_config     │    ✓    │    ✓    │    ✗    │      ✗     │
│ trend_analyze       │    ✓    │    ✓    │    ✓    │      ✗     │
│ multi_role_analyze  │    ✓    │    ✓    │    ✓    │      ✗     │
│ chatrooms_read      │    ✓    │    ✓    │    ✗    │      ✗     │
│ chatrooms_config    │    ✓    │    ✓    │    ✗    │      ✗     │
│ admin_users         │    ✓    │    ✗    │    ✗    │      ✗     │
│ admin_system        │    ✓    │    ✗    │    ✗    │      ✗     │
└─────────────────────┴─────────┴─────────┴─────────┴────────────┘
```

### 7.3 相关文件清单

**前端文件**：
- `apps/web/src/shared/ui/AppShell.vue` - 主导航组件
- `apps/web/src/app/permissions.ts` - 权限定义
- `apps/web/src/pages/system/RolePoliciesPage.vue` - 权限配置页面
- `apps/web/src/router/index.ts` - 路由配置

**后端文件**：
- `backend/server.py` - 权限校验和角色策略
- `backend/models/` - 数据模型（如需新增）

### 7.4 术语表

| 术语 | 说明 |
|------|------|
| 权限码 (Permission Code) | 具体的功能权限标识，如 `news_read` |
| 权限组 (Permission Group) | 权限的逻辑分组，如 `news` 包含多个权限码 |
| 角色 (Role) | 用户类型，如 `admin`, `pro`, `limited` |
| 角色策略 (Role Policy) | 角色的具体配置，包括权限和配额 |
| 日配额 (Daily Quota) | LLM 功能的每日调用次数限制 |

---

**文档结束**

---

> 💡 **使用建议**：
> 1. 首先实施阶段一（前端导航重构），快速见效
> 2. 观察用户反馈后再决定是否实施阶段二、三
> 3. 保持与现有系统的兼容性，避免破坏性变更
