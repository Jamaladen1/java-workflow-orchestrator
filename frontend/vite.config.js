import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// `npm run dev` proxies API calls to the Spring Boot app on :8090.
export default defineConfig({
  plugins: [react()],
  build: { outDir: 'dist', emptyOutDir: true },
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8090',
      '/health': 'http://localhost:8090',
    },
  },
});
