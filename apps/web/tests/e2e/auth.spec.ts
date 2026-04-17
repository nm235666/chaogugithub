import { test, expect } from '@playwright/test'

test.describe('认证模块', () => {
  test('登录页面展示正常', async ({ page }) => {
    await page.goto('/login')
    await page.waitForTimeout(2000)
    
    await expect(page).toHaveTitle(/web/)
    await expect(page.locator('input[type="text"]').first()).toBeVisible()
    await expect(page.locator('input[type="password"]').first()).toBeVisible()
    
    // 获取最后一个button（提交按钮）
    const buttons = page.locator('button')
    const count = await buttons.count()
    await expect(buttons.nth(count - 1)).toBeVisible()
  })

  test('管理员账号登录成功', async ({ page }) => {
    await page.goto('/login')
    await expect(page.getByPlaceholder('请输入账号（3-32位英文数字._-）')).toBeVisible({ timeout: 15000 })
    await page.getByPlaceholder('请输入账号（3-32位英文数字._-）').fill('nm235666')
    await page.getByPlaceholder('请输入密码（至少6位）').fill('nm235689')
    await page.locator('button').filter({ hasText: /^登录$/ }).last().click()
    await page.waitForURL((url) => !url.pathname.endsWith('/login'), { timeout: 15000 })
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 12000 })
  })

  test('Pro账号登录成功', async ({ page }) => {
    await page.goto('/login')
    await expect(page.getByPlaceholder('请输入账号（3-32位英文数字._-）')).toBeVisible({ timeout: 15000 })
    await page.getByPlaceholder('请输入账号（3-32位英文数字._-）').fill('zanbo')
    await page.getByPlaceholder('请输入密码（至少6位）').fill('zanbo666')
    await page.locator('button').filter({ hasText: /^登录$/ }).last().click()
    await page.waitForURL((url) => !url.pathname.endsWith('/login'), { timeout: 15000 })
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 12000 })
  })

  test('错误密码登录失败', async ({ page }) => {
    await page.goto('/login')
    await expect(page.getByPlaceholder('请输入账号（3-32位英文数字._-）')).toBeVisible({ timeout: 15000 })
    await page.getByPlaceholder('请输入账号（3-32位英文数字._-）').fill('nm235666')
    await page.getByPlaceholder('请输入密码（至少6位）').fill('wrongpassword')
    await page.locator('button').filter({ hasText: /^登录$/ }).last().click()
    await page.waitForTimeout(3000)
    expect(page.url()).toContain('/login')
  })

  test('空密码验证', async ({ page }) => {
    await page.goto('/login')
    await expect(page.getByPlaceholder('请输入账号（3-32位英文数字._-）')).toBeVisible({ timeout: 15000 })
    await page.getByPlaceholder('请输入账号（3-32位英文数字._-）').fill('nm235666')
    await page.locator('button').filter({ hasText: /^登录$/ }).last().click()
    await page.waitForTimeout(2000)
    expect(page.url()).toContain('/login')
  })
})
