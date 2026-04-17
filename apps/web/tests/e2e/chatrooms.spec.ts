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

test.describe('群聊模块', () => {
  test.beforeEach(async ({ page }) => {
    await loginAsAdmin(page)
  })

  test('群聊总览页面加载', async ({ page }) => {
    await page.goto('/chatrooms/overview')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
    expect(await page.locator('input, select').count()).toBeGreaterThan(0)
  })

  test('候选池页面加载', async ({ page }) => {
    await page.goto('/chatrooms/candidates')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('聊天记录页面加载', async ({ page }) => {
    await page.goto('/chatrooms/chatlog')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('投研结论页面加载', async ({ page }) => {
    await page.goto('/chatrooms/investment')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })
})
