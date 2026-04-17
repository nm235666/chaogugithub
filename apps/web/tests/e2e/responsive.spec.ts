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

test.describe('响应式布局测试', () => {
  test.beforeEach(async ({ page }) => {
    await loginAsAdmin(page)
  })

  test('移动端视图 - 信号总览', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/signals/overview')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('平板视图 - 股票列表', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.goto('/stocks/list')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('桌面端视图 - 决策板', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 })
    await page.goto('/research/decision')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
    const hasTable = await page.locator('table').count() > 0
    expect(hasTable).toBeTruthy()
  })
})
