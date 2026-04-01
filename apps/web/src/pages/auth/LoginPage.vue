<template>
  <div class="min-h-screen bg-[var(--bg)] px-4 py-10 text-[var(--ink)]">
    <div class="mx-auto max-w-[560px] rounded-[28px] border border-[var(--line)] bg-white p-6 shadow-[var(--shadow)]">
      <div class="text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Zanbo Quant</div>
      <h1 class="mt-2 text-3xl font-extrabold" style="font-family: var(--font-display)">账号登录</h1>
      <p class="mt-2 text-sm text-[var(--muted)]">
        支持账号注册与登录。若后端未开启鉴权，可直接继续进入。
      </p>

      <div class="mt-5 inline-flex rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] p-1 text-sm">
        <button
          class="rounded-xl px-4 py-2 font-semibold"
          :class="mode === 'login' ? 'bg-white text-[var(--ink)] shadow-sm' : 'text-[var(--muted)]'"
          @click="mode = 'login'"
        >
          登录
        </button>
        <button
          class="rounded-xl px-4 py-2 font-semibold"
          :class="mode === 'register' ? 'bg-white text-[var(--ink)] shadow-sm' : 'text-[var(--muted)]'"
          @click="mode = 'register'"
        >
          注册
        </button>
        <button
          class="rounded-xl px-4 py-2 font-semibold"
          :class="mode === 'forgot' ? 'bg-white text-[var(--ink)] shadow-sm' : 'text-[var(--muted)]'"
          @click="mode = 'forgot'"
        >
          找回密码
        </button>
      </div>

      <div class="mt-5 space-y-3">
        <label class="block text-sm font-semibold text-[var(--ink)]">账号</label>
        <input
          v-model.trim="username"
          type="text"
          autocomplete="off"
          class="w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
          placeholder="请输入账号（3-32位英文数字._-）"
          @keyup.enter="submit"
        />
        <label class="block text-sm font-semibold text-[var(--ink)]">密码</label>
        <input
          v-model.trim="password"
          type="password"
          autocomplete="off"
          class="w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
          placeholder="请输入密码（至少6位）"
          @keyup.enter="submit"
        />
        <template v-if="mode === 'forgot'">
          <label class="block text-sm font-semibold text-[var(--ink)]">重置码</label>
          <input
            v-model.trim="resetCode"
            type="text"
            autocomplete="off"
            class="w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
            placeholder="请输入重置码"
            @keyup.enter="submitResetPassword"
          />
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-2 text-sm font-semibold" :disabled="isSubmitting" @click="requestResetCode">
            获取重置码
          </button>
          <button class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-2 text-sm font-semibold" :disabled="isSubmitting" @click="submitResetPassword">
            提交新密码
          </button>
        </template>
        <template v-if="mode === 'register'">
          <label class="block text-sm font-semibold text-[var(--ink)]">邀请码</label>
          <input
            v-model.trim="inviteCode"
            type="text"
            autocomplete="off"
            class="w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
            placeholder="请输入管理员发放的邀请码"
            @keyup.enter="submit"
          />
          <label class="block text-sm font-semibold text-[var(--ink)]">确认密码</label>
          <input
            v-model.trim="confirmPassword"
            type="password"
            autocomplete="off"
            class="w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
            placeholder="请再次输入密码"
            @keyup.enter="submit"
          />
          <label class="block text-sm font-semibold text-[var(--ink)]">昵称（可选）</label>
          <input
            v-model.trim="displayName"
            type="text"
            autocomplete="off"
            class="w-full rounded-2xl border border-[var(--line)] bg-white px-4 py-3"
            placeholder="例如：研究员A"
            @keyup.enter="submit"
          />
        </template>
        <label class="inline-flex items-center gap-2 text-sm text-[var(--muted)]">
          <input v-model="rememberToken" type="checkbox" />
          记住登录（写入 localStorage）
        </label>
      </div>

      <div class="mt-5 flex flex-wrap gap-2">
        <button class="rounded-2xl bg-[var(--brand)] px-4 py-3 font-semibold text-white disabled:opacity-60" :disabled="isSubmitting || mode === 'forgot'" @click="submit">
          {{ isSubmitting ? '处理中...' : mode === 'register' ? '注册并登录' : mode === 'forgot' ? '请使用下方重置按钮' : '登录' }}
        </button>
        <button v-if="!status?.auth_required" class="rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-4 py-3 text-sm font-semibold text-[var(--ink)]" @click="enterWithoutAuth">
          无鉴权直接进入
        </button>
      </div>

      <div v-if="message" class="mt-4 rounded-2xl border border-[var(--line)] bg-[var(--panel-soft)] px-3 py-2 text-sm text-[var(--muted)]">
        {{ message }}
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from '../../stores/auth'
import {
  clearAuthStatusCache,
  fetchAuthStatus,
  forgotPassword,
  loginWithPassword,
  registerAccount,
  resetPasswordWithCode,
  type AuthStatus,
} from '../../services/api/auth'
import { clearAdminToken, readAdminToken, setAdminToken } from '../../services/authToken'

const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()

const mode = ref<'login' | 'register' | 'forgot'>('login')
const username = ref('')
const password = ref('')
const confirmPassword = ref('')
const displayName = ref('')
const inviteCode = ref('')
const resetCode = ref('')
const rememberToken = ref(true)
const isSubmitting = ref(false)
const message = ref('')
const status = ref<AuthStatus | null>(null)

function redirectToTarget() {
  const target = String(route.query.redirect || '/intelligence/global-news')
  router.replace(target.startsWith('/') ? target : '/intelligence/global-news')
}

async function submit() {
  isSubmitting.value = true
  message.value = ''
  try {
    const u = username.value.trim()
    const p = password.value.trim()
    const isRegister = mode.value === 'register'
    if (mode.value === 'forgot') {
      message.value = '请使用“获取重置码/提交新密码”按钮完成找回密码。'
      return
    }
    if (!u || !p) {
      message.value = '请输入账号和密码。'
      return
    }
    if (isRegister) {
      if (p.length < 6) {
        message.value = '密码至少 6 位。'
        return
      }
      if (p !== confirmPassword.value.trim()) {
        message.value = '两次密码输入不一致。'
        return
      }
      if (!inviteCode.value.trim()) {
        message.value = '请输入邀请码。'
        return
      }
    }
    const result = isRegister
      ? await registerAccount(u, p, displayName.value.trim(), inviteCode.value.trim())
      : await loginWithPassword(u, p)
    status.value = result
    if (result.auth_required && !result.token_valid) {
      clearAdminToken()
      message.value = '登录失败：账号或密码无效。'
      return
    }
    if (result.token) {
      setAdminToken(result.token, rememberToken.value)
    } else if (readAdminToken()) {
      setAdminToken(readAdminToken(), rememberToken.value)
    }
    clearAuthStatusCache()
    await authStore.refresh(true)
    message.value = isRegister ? '注册成功，正在进入系统...' : '登录成功，正在进入系统...'
    redirectToTarget()
  } catch (error) {
    message.value = `${mode.value === 'register' ? '注册失败' : '登录失败'}：${(error as Error).message}`
  } finally {
    isSubmitting.value = false
  }
}

async function requestResetCode() {
  const key = username.value.trim()
  if (!key) {
    message.value = '请输入账号（或邮箱）。'
    return
  }
  isSubmitting.value = true
  message.value = ''
  try {
    const resp = await forgotPassword(key)
    const dev = (resp as any)?.dev_reset_code ? `（开发重置码：${(resp as any).dev_reset_code}）` : ''
    message.value = `重置码已发送，请查看邮箱或联系管理员${dev}`
  } catch (error) {
    message.value = `获取重置码失败：${(error as Error).message}`
  } finally {
    isSubmitting.value = false
  }
}

async function submitResetPassword() {
  const u = username.value.trim()
  const p = password.value.trim()
  const c = resetCode.value.trim()
  if (!u || !p || !c) {
    message.value = '请填写账号、新密码和重置码。'
    return
  }
  isSubmitting.value = true
  message.value = ''
  try {
    await resetPasswordWithCode(u, c, p)
    message.value = '密码重置成功，请切换到登录。'
    mode.value = 'login'
    resetCode.value = ''
  } catch (error) {
    message.value = `密码重置失败：${(error as Error).message}`
  } finally {
    isSubmitting.value = false
  }
}

function enterWithoutAuth() {
  clearAdminToken()
  clearAuthStatusCache()
  authStore.loaded = false
  redirectToTarget()
}

onMounted(async () => {
  try {
    status.value = await fetchAuthStatus(true)
    if (status.value.auth_required && status.value.token_valid) {
      redirectToTarget()
      return
    }
    if (!status.value.auth_required) {
      message.value = '后端未启用管理令牌，可直接进入。'
    }
  } catch (error) {
    message.value = `鉴权状态检查失败：${(error as Error).message}`
  }
})
</script>
