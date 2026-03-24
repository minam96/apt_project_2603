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
      '/api': {
        target: 'http://localhost:3000',
        changeOrigin: false,
      },
    },
  },
})
