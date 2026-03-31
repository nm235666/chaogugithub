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

export async function downloadElementAsImage(target: HTMLElement, filename: string, width = 1600) {
  const rect = target.getBoundingClientRect()
  const scale = Math.max(2, Math.min(3, window.devicePixelRatio || 2))
  const cloned = target.cloneNode(true) as HTMLElement
  inlineComputedStyles(target, cloned)
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
    const pngUrl = canvas.toDataURL('image/png')
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
