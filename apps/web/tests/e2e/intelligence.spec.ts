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

test.describe('情报模块', () => {
  test.beforeEach(async ({ page }) => {
    await loginAsAdmin(page)
  })

  test('国际新闻页面加载', async ({ page }) => {
    await page.goto('/intelligence/global-news')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
    await expect(page.getByRole('heading', { name: '国际财经资讯' })).toBeVisible({ timeout: 15000 })
  })

  test('国内新闻页面加载', async ({ page }) => {
    await page.goto('/intelligence/cn-news')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
  })

  test('个股新闻页面加载', async ({ page }) => {
    await page.goto('/intelligence/stock-news')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
    await expect(page.locator('input, select').first()).toBeVisible({ timeout: 15000 })
  })

  test('新闻筛选功能', async ({ page }) => {
    await page.goto('/intelligence/global-news')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
    const keywordInput = page.getByPlaceholder('关键词')
    await expect(keywordInput).toBeVisible({ timeout: 15000 })
    await keywordInput.fill('test')
    await expect(keywordInput).toHaveValue('test')
    await expect(page.getByRole('heading', { name: '国际财经资讯' })).toBeVisible()
  })

  test('日报汇总页面加载', async ({ page }) => {
    await page.goto('/intelligence/daily-summaries')
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
  })
})
