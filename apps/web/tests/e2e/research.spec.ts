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

test.describe('投研模块', () => {
  test.beforeEach(async ({ page }) => {
    await loginAsAdmin(page)
  })

  test('评分总览页面加载', async ({ page }) => {
    await page.goto('/research/scoreboard')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
    await expect(page.getByText('评分总览').first()).toBeVisible({ timeout: 15000 })
    await expect(page.getByRole('heading', { name: '总览状态' })).toBeVisible({ timeout: 15000 })
    const hasStatCards = await page.locator('[class*="rounded-\[var(--radius-md)\]"]').count() > 0
    expect(hasStatCards).toBeTruthy()
  })

  test('决策板页面加载', async ({ page }) => {
    await page.goto('/research/decision')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
    await expect(page.getByText('投研决策板').first()).toBeVisible({ timeout: 15000 })
    await expect(page.getByRole('heading', { name: '决策输入' })).toBeVisible({ timeout: 15000 })
    await expect(page.getByRole('button', { name: /刷新决策板|刷新中/ }).first()).toBeVisible({ timeout: 15000 })
  })

  test('研究报告页面加载', async ({ page }) => {
    await page.goto('/research/reports')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
  })

  test('多角色研究页面加载', async ({ page }) => {
    await page.goto('/research/multi-role')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
  })

  test('趋势分析页面加载', async ({ page }) => {
    await page.goto('/research/trend')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
  })

  test('决策快照按钮具备 pending 状态与结果反馈', async ({ page }) => {
    await page.route('**/api/decision/snapshot/run', async (route) => {
      await page.waitForTimeout(900)
      await route.continue()
    })

    await page.goto('/research/decision')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
    const snapshotButton = page.getByRole('button', { name: /生成快照|生成中/ }).first()
    await snapshotButton.click()
    await expect(snapshotButton).toBeDisabled()
    await expect(snapshotButton).toHaveText(/生成中/)
    await page.waitForTimeout(2500)
    const hasSuccess = await page.locator('text=快照已生成').first().isVisible().catch(() => false)
    const hasFailure = await page.locator('text=快照生成失败').first().isVisible().catch(() => false)
    expect(hasSuccess || hasFailure).toBeTruthy()
    if (hasSuccess) {
      const hasId = await page.locator('text=结果标识').first().isVisible().catch(() => false)
      expect(hasId).toBeTruthy()
    }
  })
})
