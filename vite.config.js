import { defineConfig } from 'vite'
import { resolve } from 'path'

export default defineConfig({
  build: {
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        calc: resolve(__dirname, 'calc.html'),
      },
    },
  },
  server: {
    port: 5173,
    watch: {
      ignored: [
        '**/_ref_*/**',
        '**/logs/**',
        '**/.github/logs/**',
        '**/tasks/**',
      ],
    },
    proxy: {
      // 실거래 분석 탭 — 아파트매매 실거래가 API
      '/api/trade': {
        target: 'https://apis.data.go.kr',
        changeOrigin: true,
        rewrite: (path) =>
          path.replace('/api/trade', '/1613000/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'),
      },
      // 실거래 분석 탭 — 건축물대장 표제부 API
      '/api/building': {
        target: 'https://apis.data.go.kr',
        changeOrigin: true,
        rewrite: (path) =>
          path.replace('/api/building', '/1613000/BldRgstHubService/getBrTitleInfo'),
      },
      // 기존 server.js 프록시 (실거래가·시세추이 탭)
      '/api/molit': {
        target: 'http://localhost:3000',
        changeOrigin: false,
      },
      '/api/config': {
        target: 'http://localhost:3000',
        changeOrigin: false,
      },
      '/api/seoul': {
        target: 'http://localhost:3000',
        changeOrigin: false,
      },
    },
  },
})
