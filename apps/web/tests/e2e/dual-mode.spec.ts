import { expect, test, type Page } from '@playwright/test'
import { skipUnlessSmokeLimitedRoleIsLimited } from './helpers/smokeLimitedRole'

const credentials = {
  admin: {
    username: process.env.SMOKE_ADMIN_USERNAME?.trim() || 'nm235666',
    password: process.env.SMOKE_ADMIN_PASSWORD?.trim() || 'nm235689',
  },
  pro: {
    username: process.env.SMOKE_PRO_USERNAME?.trim() || 'zanbo',
    password: process.env.SMOKE_PRO_PASSWORD?.trim() || 'zanbo666',
  },
  limited: {
    username: process.env.SMOKE_LIMITED_USERNAME?.trim() || 'test112233',
    password: process.env.SMOKE_LIMITED_PASSWORD?.trim() || 'test123',
  },
}

async function login(page: Page, role: keyof typeof credentials) {
  const account = credentials[role]
  await page.goto('/login')
  await page.getByPlaceholder('请输入账号（3-32位英文数字._-）').fill(account.username)
  await page.getByPlaceholder('请输入密码（至少6位）').fill(account.password)
  await page.locator('button').filter({ hasText: /^登录$/ }).last().click()
  await page.waitForURL((url) => !url.pathname.endsWith('/login'))
}

test.describe('双模式 smoke', () => {
  test('admin 壳层与用户壳层的导航互不混入', async ({ page }) => {
    await login(page, 'admin')
    await page.goto('/admin/dashboard')
    await expect(page.locator('[data-shell-surface="admin"]')).toBeVisible()
    await expect(page.locator('[data-shell-nav="admin"]')).toContainText('系统治理')
    await expect(page.locator('[data-shell-nav="admin"]')).not.toContainText('研究输入')

    await page.goto('/app/workbench')
    await expect(page.locator('[data-shell-surface="app"]')).toBeVisible()
    await expect(page.locator('[data-shell-nav="app"]')).toContainText('工作台')
    await expect(page.locator('[data-shell-nav="app"]')).not.toContainText('系统治理')
  })

  test('admin 登录后默认落点为 /admin/dashboard', async ({ page }) => {
    await login(page, 'admin')
    await expect(page).toHaveURL(/\/admin\/dashboard$/)
  })

  test('pro 登录后默认落点为 /app/workbench', async ({ page }) => {
    await login(page, 'pro')
    await expect(page).toHaveURL(/\/app\/workbench$/)
  })
})

test.describe('旧路径兼容', () => {
  test.beforeEach(async ({ page }) => {
    await login(page, 'admin')
  })

  const legacyCases: Array<{ legacy: string; expect: RegExp }> = [
    { legacy: '/dashboard', expect: /\/admin\/dashboard$/ },
    { legacy: '/research/workbench', expect: /\/app\/workbench$/ },
    { legacy: '/research/funnel', expect: /\/app\/funnel$/ },
    { legacy: '/research/decision', expect: /\/app\/decision$/ },
    { legacy: '/portfolio/positions', expect: /\/app\/positions$/ },
    { legacy: '/portfolio/orders', expect: /\/app\/orders$/ },
    { legacy: '/portfolio/review', expect: /\/app\/review$/ },
    { legacy: '/portfolio/allocation', expect: /\/app\/allocation$/ },
    { legacy: '/market/conclusion', expect: /\/app\/market$/ },
    { legacy: '/macro', expect: /\/app\/macro$/ },
    { legacy: '/macro/regime', expect: /\/app\/macro-regime$/ },
    { legacy: '/stocks/list', expect: /\/app\/stocks\/list$/ },
    { legacy: '/stocks/scores', expect: /\/app\/stocks\/scores$/ },
    { legacy: '/intelligence/global-news', expect: /\/app\/intelligence\/global-news$/ },
    { legacy: '/intelligence/cn-news', expect: /\/app\/intelligence\/cn-news$/ },
    { legacy: '/signals/overview', expect: /\/app\/signals\/overview$/ },
    { legacy: '/signals/themes', expect: /\/app\/signals\/themes$/ },
    { legacy: '/signals/timeline', expect: /\/app\/signals\/timeline$/ },
    { legacy: '/app/themes', expect: /\/app\/signals\/themes$/ },
    { legacy: '/app/stocks', expect: /\/app\/stocks\/list$/ },
    { legacy: '/signals/audit', expect: /\/admin\/system\/signals-audit$/ },
    { legacy: '/signals/quality-config', expect: /\/admin\/system\/signals-quality$/ },
    { legacy: '/signals/state-timeline', expect: /\/admin\/system\/signals-state-timeline$/ },
    { legacy: '/system/source-monitor', expect: /\/admin\/system\/source-monitor$/ },
    { legacy: '/system/jobs-ops', expect: /\/admin\/system\/jobs-ops$/ },
    { legacy: '/system/llm-providers', expect: /\/admin\/system\/llm-providers$/ },
    { legacy: '/system/permissions', expect: /\/admin\/system\/permissions$/ },
    { legacy: '/system/database-audit', expect: /\/admin\/system\/database-audit$/ },
    { legacy: '/system/invites', expect: /\/admin\/system\/invites$/ },
    { legacy: '/system/users', expect: /\/admin\/system\/users$/ },
    { legacy: '/chatrooms/overview', expect: /\/app\/chatrooms\/overview$/ },
    { legacy: '/chatrooms/candidates', expect: /\/app\/chatrooms\/candidates$/ },
    { legacy: '/chatrooms/investment', expect: /\/app\/chatrooms\/investment$/ },
  ]

  for (const c of legacyCases) {
    test(`${c.legacy} 转向新地址`, async ({ page }) => {
      await page.goto(c.legacy)
      await expect(page).toHaveURL(c.expect)
    })
  }

  test('旧路径带查询参数时参数保留', async ({ page }) => {
    await page.goto('/stocks/detail/000001.SZ?tab=prices')
    await expect(page).toHaveURL(/\/app\/stocks\/detail\/000001\.SZ\?tab=prices$/)
  })
})

test.describe('模式切换与上下文保留', () => {
  test('admin 由 app 切到 admin，保留可共享上下文，丢弃 admin 内部参数', async ({ page }) => {
    await login(page, 'admin')
    await page.goto('/app/stocks/scores?ts_code=000001.SZ&job_run_id=ignore_me')
    await expect(page.locator('[data-shell-surface="app"]')).toBeVisible()
    const switchBtn = page.locator('[data-shell-switch="app"]')
    await expect(switchBtn).toBeVisible()
    await switchBtn.click()
    await page.waitForURL((url) => url.pathname.startsWith('/admin/'))
    const current = new URL(page.url())
    // admin has no stock detail page, so degrades to /admin/dashboard
    expect(current.pathname.startsWith('/admin/')).toBeTruthy()
    // Admin-internal keys must be dropped
    expect(current.searchParams.get('job_run_id')).toBeNull()
  })

  test('admin 由 admin 切回 app，对象上下文保留并落到对象页', async ({ page }) => {
    await login(page, 'admin')
    await page.goto('/admin/dashboard?ts_code=000001.SZ&provider_id=drop_me')
    await expect(page.locator('[data-shell-surface="admin"]')).toBeVisible()
    const switchBtn = page.locator('[data-shell-switch="admin"]')
    await expect(switchBtn).toBeVisible()
    await switchBtn.click()
    await page.waitForURL((url) => url.pathname.startsWith('/app/'))
    const current = new URL(page.url())
    expect(current.pathname).toContain('/app/stocks/detail/')
    expect(current.pathname).toContain('000001.SZ')
    expect(current.searchParams.get('provider_id')).toBeNull()
  })

  test('模式切换后弹出提示 toast', async ({ page }) => {
    await login(page, 'admin')
    await page.goto('/admin/dashboard')
    await page.locator('[data-shell-switch="admin"]').click()
    await expect(page.locator('body')).toContainText(/已切换到研究工作台/)
  })

  test('pro 无 admin 权限时不显示模式切换按钮', async ({ page }) => {
    await login(page, 'pro')
    await page.goto('/app/workbench')
    await expect(page.locator('[data-shell-switch="app"]')).toHaveCount(0)
  })
})

test.describe('权限分流与 upgrade 模式解释', () => {
  test.describe('limited 账号路径', () => {
    test.beforeEach(async ({ request }) => {
      await skipUnlessSmokeLimitedRoleIsLimited(request)
    })

    test('limited 账号访问 admin 页跳到 upgrade 并指示 admin 模式', async ({ page }) => {
      await login(page, 'limited')
      await page.goto('/admin/system/jobs-ops')
      await expect(page).toHaveURL(/\/upgrade/)
      const banner = page.locator('[data-upgrade-blocked="true"]')
      await expect(banner).toBeVisible()
      await expect(banner).toHaveAttribute('data-upgrade-mode', 'admin')
      await expect(banner).toContainText('后台管理模式')
    })

    test('limited 账号访问 app 研究页跳到 upgrade 并指示 app 模式', async ({ page }) => {
      await login(page, 'limited')
      await page.goto('/app/workbench')
      await expect(page).toHaveURL(/\/upgrade/)
      const banner = page.locator('[data-upgrade-blocked="true"]')
      await expect(banner).toBeVisible()
      await expect(banner).toHaveAttribute('data-upgrade-mode', 'app')
      await expect(banner).toContainText('用户模式')
    })
  })

  test('研究角色主链 CTA 不借道 admin 页', async ({ page }) => {
    await login(page, 'pro')
    await page.goto('/app/workbench')
    await expect(page.locator('#main-content a[href^="/admin/"]')).toHaveCount(0)
  })

  test('研究角色决策板主链 CTA 不借道 admin 页', async ({ page }) => {
    await login(page, 'pro')
    await page.goto('/app/decision')
    await expect(page.locator('#main-content a[href^="/admin/"]')).toHaveCount(0)
  })

  test('admin 主链 CTA 不借道 app 页', async ({ page }) => {
    await login(page, 'admin')
    await page.goto('/admin/dashboard')
    await expect(page.locator('#main-content a[href^="/app/"]')).toHaveCount(0)
  })

  test('admin jobs-ops 主链 CTA 不借道 app 页', async ({ page }) => {
    await login(page, 'admin')
    await page.goto('/admin/system/jobs-ops')
    await expect(page.locator('#main-content a[href^="/app/"]')).toHaveCount(0)
  })
})
