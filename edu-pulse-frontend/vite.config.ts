import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// Setting up my React and Tailwind compiler pipelines natively
export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],
})