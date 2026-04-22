import { http } from '../http'

export async function fetchChatrooms(params: Record<string, any>) {
  const { data } = await http.get('/api/chatrooms', { params })
  return data
}

export async function triggerChatroomFetch(room_id: string, mode = 'today') {
  const { data } = await http.get('/api/chatrooms/fetch', {
    params: { room_id, mode },
  })
  return data
}

export async function fetchWechatChatlog(params: Record<string, any>) {
  const { data } = await http.get('/api/wechat-chatlog', { params })
  return data
}

export async function fetchChatroomInvestment(params: Record<string, any>) {
  const { data } = await http.get('/api/chatrooms/investment', { params })
  return data
}

export async function fetchCandidatePool(params: Record<string, any>) {
  const { data } = await http.get('/api/chatrooms/candidate-pool', { params })
  return data
}

export async function fetchChatroomAccuracy(params: Record<string, any>) {
  const { data } = await http.get('/api/chatrooms/accuracy', { params })
  return data
}

export async function fetchChatroomRoomDetail(params: Record<string, any>) {
  const { data } = await http.get('/api/chatrooms/room-detail', { params })
  return data
}

export async function fetchChatroomSenderDetail(params: Record<string, any>) {
  const { data } = await http.get('/api/chatrooms/sender-detail', { params })
  return data
}
