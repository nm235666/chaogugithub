import { defineStore } from 'pinia'

export const useRealtimeStore = defineStore('realtime', {
  state: () => ({
    connected: false,
    lastEvent: '',
    lastPayloadAt: '',
  }),
  actions: {
    setConnected(value: boolean) {
      this.connected = value
    },
    pushEvent(event: string, payloadAt = '') {
      this.lastEvent = event
      this.lastPayloadAt = payloadAt
    },
  },
})
