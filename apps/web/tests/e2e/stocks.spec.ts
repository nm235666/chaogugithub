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

test.describe('股票模块', () => {
  test.beforeEach(async ({ page }) => {
    await loginAsAdmin(page)
  })

  test('股票列表页面加载', async ({ page }) => {
    await page.goto('/stocks/list')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
    const hasTable = await page.locator('table, .data-table').count() > 0
    const hasCards = await page.locator('.info-card, [class*="InfoCard"]').count() > 0
    expect(hasTable || hasCards).toBeTruthy()
  })

  test('股票评分页面加载', async ({ page }) => {
    await page.goto('/stocks/scores')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('股票价格页面加载', async ({ page }) => {
    await page.goto('/stocks/prices')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('股票搜索功能', async ({ page }) => {
    await page.goto('/stocks/list')
    await page.waitForTimeout(3000)
    const searchInput = page.locator('input[type="text"]').first()
    if (await searchInput.count() > 0) {
      await searchInput.fill('000001')
      await searchInput.press('Enter')
      await page.waitForTimeout(3000)
    }
    await expect(page.locator('body')).toBeVisible()
  })

  test('股票详情页面加载', async ({ page }) => {
    await page.goto('/stocks/detail/000001.SZ')
    await page.waitForTimeout(4000)
    await expect(page.locator('body')).toBeVisible()
  })

  test('分页超界自动回收并同步 URL', async ({ page }) => {
    await page.goto('/stocks/list?keyword=%E5%B9%B3%E5%AE%89&page=999&page_size=20')
    await page.waitForTimeout(4500)
    const pager = page.locator('.table-pager').first()
    await expect(pager).toBeVisible()
    const pagerText = (await pager.innerText()).trim()
    const match = pagerText.match(/第\s*(\d+)\s*\/\s*(\d+)\s*页/)
    expect(match).toBeTruthy()
    const currentPage = Number(match?.[1] || 0)
    const totalPages = Number(match?.[2] || 0)
    expect(currentPage).toBeGreaterThan(0)
    expect(totalPages).toBeGreaterThan(0)
    expect(currentPage).toBeLessThanOrEqual(totalPages)
    await expect(page).not.toHaveURL(/page=999/)
  })

  test('评分页跳转决策板时携带上下文', async ({ page }) => {
    await page.goto('/stocks/scores?industry=%E9%93%B6%E8%A1%8C&keyword=%E5%B9%B3%E5%AE%89&score_date=20260414')
    await page.waitForTimeout(3000)
    const decisionLink = page.getByRole('link', { name: '打开决策板' })
    await expect(decisionLink).toHaveAttribute('href', /\/research\/decision/)
    await expect(decisionLink).toHaveAttribute('href', /industry=%E9%93%B6%E8%A1%8C/)
    await expect(decisionLink).toHaveAttribute('href', /keyword=%E5%B9%B3%E5%AE%89/)
    await expect(decisionLink).toHaveAttribute('href', /score_date=20260414/)
    await decisionLink.click()
    await page.waitForURL(/\/research\/decision/, { timeout: 15000 })
    await expect(page.locator('#main-content')).toBeVisible({ timeout: 15000 })
    await expect(page.locator('text=当前研究上下文').first()).toBeVisible()
    await expect(page.locator('text=行业 银行').first()).toBeVisible()
    await expect(page.locator('text=关键词 平安').first()).toBeVisible()
  })

  test('空结果时分页固定显示 1/1 且 URL 回到 page=1', async ({ page }) => {
    await page.goto('/stocks/list?keyword=NO_SUCH_STOCK_ABCXYZ&page=9&page_size=20')
    await page.waitForTimeout(2800)
    await expect(page).toHaveURL(/page=1/, { timeout: 30000 })
    await expect(page.locator('text=当前筛选结果为空').first()).toBeVisible()
    await expect(page.locator('text=第 1 / 1 页').first()).toBeVisible()
  })
})
