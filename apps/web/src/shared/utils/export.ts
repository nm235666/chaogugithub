function safeFilename(name: string) {
  return String(name || 'export')
    .replace(/[\\/:*?"<>|]+/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
}

function inlineComputedStyles(source: Element, target: Element) {
  const computed = window.getComputedStyle(source)
  const cssText = Array.from(computed)
    .map((name) => `${name}:${computed.getPropertyValue(name)};`)
    .join('')
  target.setAttribute('style', cssText)
  const sourceChildren = Array.from(source.children)
  const targetChildren = Array.from(target.children)
  sourceChildren.forEach((child, index) => {
    const next = targetChildren[index]
    if (child instanceof Element && next instanceof Element) {
      inlineComputedStyles(child, next)
    }
  })
}

function blobToDataUrl(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => resolve(String(reader.result || ''))
    reader.onerror = () => reject(new Error('读取图片数据失败'))
    reader.readAsDataURL(blob)
  })
}

function stripExternalStyleUrls(root: HTMLElement) {
  const nodes = [root, ...Array.from(root.querySelectorAll<HTMLElement>('*'))]
  const urlRegex = /url\(([^)]+)\)/gi
  for (const node of nodes) {
    const styleText = node.getAttribute('style')
    if (!styleText) continue
    const cleaned = styleText.replace(urlRegex, (_full, raw) => {
      const value = String(raw || '').replace(/["']/g, '').trim()
      if (!value) return 'none'
      if (value.startsWith('data:')) return `url(${value})`
      return 'none'
    })
    node.setAttribute('style', cleaned)
  }
}

async function embedCrossOriginImages(root: HTMLElement) {
  const imgs = Array.from(root.querySelectorAll<HTMLImageElement>('img'))
  for (const img of imgs) {
    const rawSrc = String(img.getAttribute('src') || '').trim()
    if (!rawSrc) continue
    if (rawSrc.startsWith('data:')) continue
    try {
      const absUrl = new URL(rawSrc, window.location.href)
      // 同源资源保留原地址，避免额外请求
      if (absUrl.origin === window.location.origin) continue
      const resp = await fetch(absUrl.toString(), { mode: 'cors', credentials: 'omit' })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
      const blob = await resp.blob()
      const dataUrl = await blobToDataUrl(blob)
      img.src = dataUrl
    } catch {
      // 跨域不可读时直接隐藏，避免污染 canvas
      img.setAttribute('data-export-skipped', '1')
      img.style.display = 'none'
    }
  }
}

export function downloadTextFile(content: string, filename: string, mime = 'text/plain;charset=utf-8') {
  const blob = new Blob([content], { type: mime })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = safeFilename(filename)
  document.body.appendChild(link)
  link.click()
  link.remove()
  window.setTimeout(() => URL.revokeObjectURL(url), 1000)
}

function stripImageNodes(root: HTMLElement) {
  const nodes = root.querySelectorAll('img, picture, source, video')
  nodes.forEach((node) => node.remove())
}

function downloadCanvas(canvas: HTMLCanvasElement, filename: string) {
  const url = canvas.toDataURL('image/png')
  const link = document.createElement('a')
  link.href = url
  link.download = safeFilename(filename)
  document.body.appendChild(link)
  link.click()
  link.remove()
}

function downloadTextFallbackImage(target: HTMLElement, filename: string, width = 1600) {
  const text = (target.innerText || target.textContent || '').trim()
  const lines = (text || '导出内容为空')
    .replace(/\r/g, '')
    .split('\n')
    .flatMap((line) => {
      const trimmed = line.trim()
      if (!trimmed) return ['']
      const chunks: string[] = []
      let i = 0
      const size = 56
      while (i < trimmed.length) {
        chunks.push(trimmed.slice(i, i + size))
        i += size
      }
      return chunks
    })
  const lineHeight = 34
  const padding = 48
  const maxLines = Math.min(lines.length, 400)
  const height = Math.max(400, padding * 2 + maxLines * lineHeight)
  const canvas = document.createElement('canvas')
  canvas.width = width
  canvas.height = height
  const ctx = canvas.getContext('2d')
  if (!ctx) throw new Error('无法创建画布上下文')
  ctx.fillStyle = '#ffffff'
  ctx.fillRect(0, 0, width, height)
  ctx.fillStyle = '#111827'
  ctx.font = '24px sans-serif'
  let y = padding
  for (let i = 0; i < maxLines; i += 1) {
    ctx.fillText(lines[i] || '', padding, y)
    y += lineHeight
  }
  downloadCanvas(canvas, filename)
}

export async function downloadElementAsImage(
  target: HTMLElement,
  filename: string,
  width = 1600,
  options?: { stripImages?: boolean },
) {
  const rect = target.getBoundingClientRect()
  const scale = Math.max(2, Math.min(3, window.devicePixelRatio || 2))
  const cloned = target.cloneNode(true) as HTMLElement
  inlineComputedStyles(target, cloned)
  if (options?.stripImages) {
    stripImageNodes(cloned)
  }
  await embedCrossOriginImages(cloned)
  stripExternalStyleUrls(cloned)
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${Math.ceil((rect.height / Math.max(rect.width, 1)) * width)}">
      <foreignObject x="0" y="0" width="100%" height="100%">
        <div xmlns="http://www.w3.org/1999/xhtml" style="width:${width}px;transform-origin:top left;transform:scale(${width / Math.max(rect.width, 1)});">
          ${new XMLSerializer().serializeToString(cloned)}
        </div>
      </foreignObject>
    </svg>
  `.trim()
  const svgBlob = new Blob([svg], { type: 'image/svg+xml;charset=utf-8' })
  const svgUrl = URL.createObjectURL(svgBlob)
  try {
    const image = new Image()
    image.decoding = 'async'
    image.crossOrigin = 'anonymous'
    const loaded = new Promise<void>((resolve, reject) => {
      image.onload = () => resolve()
      image.onerror = () => reject(new Error('图片渲染失败'))
    })
    image.src = svgUrl
    await loaded
    const canvas = document.createElement('canvas')
    canvas.width = Math.ceil(image.width * scale)
    canvas.height = Math.ceil(image.height * scale)
    const ctx = canvas.getContext('2d')
    if (!ctx) throw new Error('无法创建画布上下文')
    ctx.scale(scale, scale)
    ctx.drawImage(image, 0, 0)
    let pngUrl = ''
    try {
      pngUrl = canvas.toDataURL('image/png')
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error || '未知错误')
      if (message.toLowerCase().includes('tainted canvases')) {
        // 最终兜底：退化为纯文本截图，保证可下载
        downloadTextFallbackImage(target, filename, width)
        return
      }
      throw error
    }
    const link = document.createElement('a')
    link.href = pngUrl
    link.download = safeFilename(filename)
    document.body.appendChild(link)
    link.click()
    link.remove()
  } finally {
    URL.revokeObjectURL(svgUrl)
  }
}
