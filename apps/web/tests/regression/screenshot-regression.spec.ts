/**
 * Screenshot regression archive for high-risk pages.
 * Purpose: 为图谱、决策板、用户管理写操作三类高风险页留存截图档案，
 *          用于后续回归对比。截图存入 test-results/screenshots/。
 *
 * Run: npx playwright test tests/e2e/screenshot-regression.spec.ts
 */

import { expect, test, type Page } from '@playwright/test'
import path from 'path'
import fs from 'fs'

const credentials = {
  admin: {
    username: process.env.SMOKE_ADMIN_USERNAME?.trim() || 'nm235666',
    password: process.env.SMOKE_ADMIN_PASSWORD?.trim() || 'nm235689',
  },
  pro: {
    username: process.env.SMOKE_PRO_USERNAME?.trim() || 'zanbo',
    password: process.env.SMOKE_PRO_PASSWORD?.trim() || 'zanbo666',
  },
}

const screenshotsDir = path.resolve('test-results/screenshots')

async function login(page: Page, role: keyof typeof credentials) {
  const account = credentials[role]
  await page.goto('/login')
  await page.getByPlaceholder('请输入账号（3-32位英文数字._-）').fill(account.username)
  await page.getByPlaceholder('请输入密码（至少6位）').fill(account.password)
  await page.locator('button').filter({ hasText: /^登录$/ }).last().click()
  await page.waitForURL((url) => !url.pathname.endsWith('/login'))
}

async function saveScreenshot(page: Page, name: string) {
  fs.mkdirSync(screenshotsDir, { recursive: true })
  const datestamp = new Date().toISOString().slice(0, 10)
  const filePath = path.join(screenshotsDir, `${datestamp}_${name}.png`)
  await page.screenshot({ path: filePath, fullPage: false })
  console.log(`[screenshot] saved: ${filePath}`)
  return filePath
}

test.describe('高风险页截图回归存档', () => {
  test('决策板 (research/decision) 截图存档', async ({ page }) => {
    await login(page, 'admin')
    await page.goto('/research/decision')
    await expect(page).toHaveURL(/\/research\/decision/)
    // Wait for page content to stabilize
    await page.waitForLoadState('networkidle')
    await expect(page.locator('header').filter({ hasText: /投研决策板|决策/ }).first()).toBeVisible()
    await saveScreenshot(page, 'decision-board-admin')
  })

  test('信号图谱 (signals/graph) 截图存档', async ({ page }) => {
    await login(page, 'admin')
    await page.goto('/signals/graph')
    await expect(page).toHaveURL(/\/signals\/graph/)
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(2000) // allow chart render
    await saveScreenshot(page, 'signal-chain-graph-admin')
    // Verify page rendered (header or canvas visible)
    const hasHeader = await page.locator('header').filter({ hasText: /产业链图谱|图谱/ }).first().isVisible().catch(() => false)
    const hasCanvas = await page.locator('canvas').first().isVisible().catch(() => false)
    expect(hasHeader || hasCanvas).toBe(true)
  })

  test('用户管理写操作 (system/users) 截图存档', async ({ page }) => {
    await login(page, 'admin')
    await page.goto('/system/users')
    await expect(page).toHaveURL(/\/system\/users/)
    await page.waitForLoadState('networkidle')
    await expect(page.locator('header').filter({ hasText: /用户与会话管理|用户/ }).first()).toBeVisible()
    await saveScreenshot(page, 'user-admin-write-admin')
  })

  test('pro 角色决策板截图存档', async ({ page }) => {
    await login(page, 'pro')
    await expect(page).toHaveURL(/\/research\/decision/)
    await page.waitForLoadState('networkidle')
    await saveScreenshot(page, 'decision-board-pro')
  })
})
