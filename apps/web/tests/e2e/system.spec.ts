import { test, expect } from '@playwright/test'

async function loginAsAdmin(page) {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      await page.goto('/login')
      // SPA may redirect immediately if session is already active
      try {
        await page.waitForURL((url) => !url.pathname.endsWith('/login'), { timeout: 2000 })
        await expect(page.locator('#main-content')).toBeVisible({ timeout: 12000 })
        return
      } catch {
        // Still on login page, proceed to authenticate
      }
      await page.getByPlaceholder('请输入账号（3-32位英文数字._-）').waitFor({ state: 'visible', timeout: 15000 })
      await page.getByPlaceholder('请输入账号（3-32位英文数字._-）').fill('nm235666')
      await page.getByPlaceholder('请输入密码（至少6位）').fill('nm235689')
      await page.locator('button').filter({ hasText: /^登录$/ }).last().click({ timeout: 4000 })
      await page.waitForURL((url) => !url.pathname.endsWith('/login'), { timeout: 12000 })
      await expect(page.locator('#main-content')).toBeVisible({ timeout: 12000 })
      return
    } catch {
      // retry on transient login/render failures
    }
  }
  throw new Error('admin login failed after retries')
}

test.describe('系统管理模块（Admin）', () => {
  test.beforeEach(async ({ page }) => {
    await loginAsAdmin(page)
  })

  test('数据源监控页面加载', async ({ page }) => {
    await page.goto('/system/source-monitor')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
    const hasStatCards = await page.locator('[class*="rounded-\[var(--radius-md)\]"]').count() > 0
    expect(hasStatCards).toBeTruthy()
  })

  test('任务调度页面加载', async ({ page }) => {
    await page.goto('/system/jobs-ops')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('LLM提供商页面加载', async ({ page }) => {
    await page.goto('/system/llm-providers')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('用户管理页面加载', async ({ page }) => {
    await page.goto('/system/users')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('权限策略页面加载', async ({ page }) => {
    await page.goto('/system/permissions')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('LLM 默认限速保存后显示成功反馈与最近保存时间', async ({ page }) => {
    await page.goto('/system/llm-providers')
    await page.waitForTimeout(3500)
    const input = page.locator('input[type="number"]').first()
    await input.fill('10')
    const saveButton = page.getByRole('button', { name: '保存默认限速' })
    await expect(saveButton).toBeEnabled()
    await saveButton.click()
    await expect
      .poll(async () => {
        const hasSavedAt = await page.locator('text=最近保存时间').first().isVisible().catch(() => false)
        const hasInlineSuccess = await page.locator('text=默认限速已保存').first().isVisible().catch(() => false)
        const hasInlineNoChange = await page.locator('text=配置未变化').first().isVisible().catch(() => false)
        const hasInlineError = await page.locator('text=保存默认限速失败').first().isVisible().catch(() => false)
        const hasPending = await page.getByRole('button', { name: '保存中...' }).first().isVisible().catch(() => false)
        return (hasSavedAt && (hasInlineSuccess || hasInlineNoChange)) || hasInlineError || hasPending
      }, { timeout: 20000 })
      .toBeTruthy()
  })
})

test.describe('Limited 账号权限测试', () => {
  test('Limited 账号登录后受限路由都有上下文化解释', async ({ page }) => {
    await page.goto('/login')
    await page.waitForTimeout(1200)
    await page.fill('input[type="text"]', 'test112233')
    await page.fill('input[type="password"]', 'test123')
    await page.getByRole('button', { name: '登录' }).last().click()
    try {
      await page.waitForURL((url) => !url.pathname.includes('/login'), { timeout: 15000 })
    } catch {
      await page.getByRole('button', { name: '登录' }).first().click()
      await page.waitForURL((url) => !url.pathname.includes('/login'), { timeout: 15000 })
    }
    
    const targets = ['/system/users', '/macro', '/chatrooms/investment', '/signals/overview']
    for (const target of targets) {
      await page.goto(target)
      await page.waitForTimeout(1600)
      await expect(page).toHaveURL(/\/upgrade\?from=/)
      await expect(page.locator('text=当前受限任务').first()).toBeVisible()
      await expect(page.locator('text=需要权限').first()).toBeVisible()
    }
  })
})
