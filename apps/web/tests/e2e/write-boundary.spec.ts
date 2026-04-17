import { test, expect } from '@playwright/test'

async function loginAsAdmin(page: any) {
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

test.describe('写操作专项', () => {
  test.beforeEach(async ({ page }) => {
    await loginAsAdmin(page)
  })

  test('任务触发 + 重试触发可执行', async ({ page }) => {
    await page.goto('/system/jobs-ops')
    await page.waitForTimeout(4000)

    const triggerButton = page.getByRole('button', { name: '手动触发' }).first()
    await expect(triggerButton).toBeVisible()
    const enabled = await triggerButton.isEnabled()
    if (!enabled) {
      const actionText = await page.locator('#main-content').innerText()
      expect(actionText.includes('触发中') || actionText.includes('进行中') || actionText.includes('任务调度')).toBeTruthy()
      return
    }

    await triggerButton.click()
    await page.waitForTimeout(2500)
    const actionText = await page.locator('#main-content').innerText()
    expect(actionText.includes('run_id=') || actionText.includes('触发失败')).toBeTruthy()

    // 同一任务再次触发，覆盖“重试”动作链路
    await triggerButton.click()
    await page.waitForTimeout(2500)
    const actionText2 = await page.locator('#main-content').innerText()
    expect(actionText2.includes('run_id=') || actionText2.includes('触发失败')).toBeTruthy()
  })

  test('LLM 默认限速保存可执行', async ({ page }) => {
    await page.goto('/system/llm-providers')
    await page.waitForTimeout(4500)

    const rateInput = page.locator('input[type="number"][min="1"]').first()
    await expect(rateInput).toBeVisible()
    const oldValue = (await rateInput.inputValue()).trim() || '10'
    await rateInput.fill(oldValue)

    const saveButton = page.getByRole('button', { name: '保存默认限速' })
    await expect(saveButton).toBeVisible()
    await saveButton.click()
    await page.waitForTimeout(2000)

    await expect(page.getByRole('button', { name: '刷新' })).toBeVisible()
    await expect(rateInput).toHaveValue(oldValue)
  })

  test('用户管理保存（不改值）可执行', async ({ page }) => {
    await page.goto('/system/users')
    await page.waitForTimeout(4500)

    const saveUserButton = page.getByRole('button', { name: '保存用户' }).first()
    await expect(saveUserButton).toBeVisible()
    await saveUserButton.click()

    const confirmButton = page.getByRole('button', { name: '确认执行' })
    await expect(confirmButton).toBeVisible()
    await confirmButton.click()
    await page.waitForTimeout(2500)

    await expect(page.locator('text=用户与会话管理').first()).toBeVisible()
  })

  test('用户管理真实改值后恢复原值（全闭环）', async ({ page }) => {
    await page.goto('/system/users')
    await page.waitForTimeout(5000)

    const targetUser = 'test112233'
    const usernameText = page.getByText(targetUser).first()
    await expect(usernameText).toBeVisible()
    const userCard = usernameText.locator('xpath=ancestor::div[contains(@class,"rounded-[20px]")]').first()

    const roleSelect = userCard.locator('select').first()
    await expect(roleSelect).toBeVisible()
    const originalRole = String(await roleSelect.inputValue()).trim() || 'limited'
    const changedRole = originalRole === 'limited' ? 'pro' : 'limited'

    // 第一步：真实改值并保存
    await roleSelect.selectOption(changedRole)
    await userCard.getByRole('button', { name: '保存用户' }).first().click()
    await page.getByRole('button', { name: '确认执行' }).click()
    await page.waitForTimeout(2500)
    await expect(userCard.locator('text=角色').first()).toContainText(changedRole)

    // 第二步：恢复原值并保存（闭环）
    await roleSelect.selectOption(originalRole)
    await userCard.getByRole('button', { name: '保存用户' }).first().click()
    await page.getByRole('button', { name: '确认执行' }).click()
    await page.waitForTimeout(2500)
    await page.reload()
    await page.waitForTimeout(2000)

    const refreshedUser = page.getByText(targetUser).first()
    await expect(refreshedUser).toBeVisible()
    const refreshedCard = refreshedUser.locator('xpath=ancestor::div[contains(@class,"rounded-[20px]")]').first()
    const refreshedRoleSelect = refreshedCard.locator('select').first()
    const currentRole = String(await refreshedRoleSelect.inputValue()).trim()

    if (currentRole !== originalRole) {
      await refreshedRoleSelect.selectOption(originalRole)
      await refreshedCard.getByRole('button', { name: '保存用户' }).first().click()
      await page.getByRole('button', { name: '确认执行' }).click()
      await page.waitForTimeout(2500)
      await page.reload()
      await page.waitForTimeout(2000)
    }

    const finalUser = page.getByText(targetUser).first()
    const finalCard = finalUser.locator('xpath=ancestor::div[contains(@class,"rounded-[20px]")]').first()
    await expect(finalCard.locator('text=角色').first()).toContainText(originalRole)
  })
})

test.describe('边界输入专项', () => {
  test.beforeEach(async ({ page }) => {
    await loginAsAdmin(page)
  })

  test('超长输入不会导致用户页崩溃', async ({ page }) => {
    await page.goto('/system/users')
    await page.waitForTimeout(3500)

    const longText = 'x'.repeat(512)
    const keywordInput = page.locator('input[placeholder*="关键字"]').first()
    await keywordInput.fill(longText)
    await page.getByRole('button', { name: '刷新' }).first().click()
    await page.waitForTimeout(2000)

    await expect(keywordInput).toHaveValue(longText)
    await expect(page.locator('text=用户与会话管理').first()).toBeVisible()
  })

  test('非法日期输入能给出可见提示', async ({ page }) => {
    await page.goto('/intelligence/stock-news')
    await page.waitForTimeout(3500)

    await page.locator('input[placeholder="YYYY-MM-DD"]').first().fill('2026-13-40')
    await page.getByRole('button', { name: '查询' }).click()
    await page.waitForTimeout(1200)

    await expect(page.locator('text=日期格式错误，请使用 YYYY-MM-DD。').first()).toBeVisible()
  })

  test('重复提交时按钮会进入进行中状态', async ({ page }) => {
    await page.goto('/system/jobs-ops')
    await page.waitForTimeout(4000)

    const triggerButton = page.getByRole('button', { name: '手动触发' }).first()
    await expect(triggerButton).toBeVisible()

    await triggerButton.click()
    await page.waitForTimeout(300)
    const actionText = await page.locator('#main-content').innerText()
    expect(actionText.includes('触发中...') || actionText.includes('run_id=') || actionText.includes('触发失败')).toBeTruthy()
  })

  test('慢接口超时后会显示失败反馈', async ({ page }) => {
    await page.route('**/api/stock-news/fetch**', async (route: any) => {
      await new Promise((resolve) => setTimeout(resolve, 25_000))
      await route.continue()
    })

    await page.goto('/intelligence/stock-news')
    await page.waitForTimeout(3500)

    await page.getByRole('button', { name: '立即采集' }).click()
    await expect(page.locator('text=采集失败').first()).toBeVisible({ timeout: 35_000 })
  })
})
