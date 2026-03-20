import { defineConfig } from 'vitepress'
import { tabsMarkdownPlugin } from 'vitepress-plugin-tabs'
import { mathjaxPlugin } from './mathjax-plugin'
import footnote from "markdown-it-footnote";
import path from 'path'

const mathjax = mathjaxPlugin()

function getBaseRepository(base: string): string {
  if (!base || base === '/') return '/';
  const parts = base.split('/').filter(Boolean);
  return parts.length > 0 ? `/${parts[0]}/` : '/';
}

const baseTemp = {
  base: 'REPLACE_ME_DOCUMENTER_VITEPRESS',
}

const userGuideItems = [
  // { text: 'Arrays',               link: '/UserGuide/arrays' },
  // { text: 'Groups',               link: '/UserGuide/groups' },
  { text: 'Storage Backends',     link: '/UserGuide/storage' },
  // { text: 'Data Types',           link: '/UserGuide/data_types' },
  // { text: 'Codecs & Performance', link: '/UserGuide/performance' },
  { text: 'Operations', link: '/UserGuide/operations'},
  // { text: 'Sharding', link: '/UserGuide/sharding' },
  { text: 'Missing Values',       link: '/UserGuide/missings' },
]

const tutorialItems = [
  { text: 'Tutorial', link: '/tutorials/tutorial' },
  // { text: 'Reading & Writing Arrays', link: '/tutorials/read_write' },
  { text: 'Cloud Storage (S3)',       link: '/tutorials/s3examples' },
  // { text: 'Working with Groups',      link: '/tutorials/groups' },
  // { text: 'Compression & Codecs',     link: '/tutorials/codecs' },
]

const ecosystemItems = [
  { text: 'YAXArrays.jl',       link: 'https://juliadatacubes.github.io/YAXArrays.jl/stable/' },
  { text: 'DiskArrayEngine.jl', link: 'https://github.com/meggart/DiskArrayEngine.jl' },
  { text: 'DiskArrays.jl',      link: 'https://github.com/JuliaIO/DiskArrays.jl' },
  { text: 'DimensionalData.jl', link: 'https://rafaqz.github.io/DimensionalData.jl/stable/' },
  { text: 'NetCDF.jl',          link: 'https://meggart.github.io/NetCDF.jl/dev/' },
  { text: 'Makie.jl',           link: 'https://docs.makie.org/dev/' },
]

const developmentItems = [
  { text: 'Contributing',  link: '/contributing' },
  { text: 'Release Notes', link: '/changelog' },
  { text: 'API', link: '/reference' },
]

const navTemp = {
  nav: [
    { text: 'Home',        link: '/' },
    { text: 'Get Started', link: '/get_started' },
    { text: 'User Guide',  items: userGuideItems },
    { text: 'Tutorials',   items: tutorialItems },
    { text: 'Ecosystem',   items: ecosystemItems },
    { text: 'Development', items: developmentItems },
  ],
}

const nav = [
  ...navTemp.nav,
  { component: 'VersionPicker' },
]

const sidebar = [
  {
    text: 'Get Started',
    items: [
      { text: 'Installation & Quick Start', link: '/get_started' },
    ]
  },
  {
    text: 'User Guide',
    collapsed: false,
    items: userGuideItems,
  },
  {
    text: 'Tutorials',
    collapsed: false,
    items: tutorialItems,
  },
  {
    text: 'Ecosystem',
    collapsed: true,
    items: ecosystemItems,
  },
  {
    text: 'Development',
    collapsed: true,
    items: developmentItems,
  },
]

export default defineConfig({
  base: 'REPLACE_ME_DOCUMENTER_VITEPRESS',
  title: 'REPLACE_ME_DOCUMENTER_VITEPRESS',
  description: 'REPLACE_ME_DOCUMENTER_VITEPRESS',
  lastUpdated: true,
  cleanUrls: true,
  ignoreDeadLinks: true,
  outDir: 'REPLACE_ME_DOCUMENTER_VITEPRESS',

  head: [
    ['link', { rel: 'icon', href: 'REPLACE_ME_DOCUMENTER_VITEPRESS_FAVICON' }],
    ['script', { src: `${getBaseRepository(baseTemp.base)}versions.js` }],
    ['script', { src: `${baseTemp.base}siteinfo.js` }]
  ],

  markdown: {
    config(md) {
      md.use(tabsMarkdownPlugin);
      md.use(footnote);
      mathjax.markdownConfig(md);
    },
    theme: {
      light: "github-light",
      dark: "github-dark"
    },
  },

  vite: {
    plugins: [mathjax.vitePlugin],
    define: {
      __DEPLOY_ABSPATH__: JSON.stringify('REPLACE_ME_DOCUMENTER_VITEPRESS_DEPLOY_ABSPATH'),
    },
    resolve: {
      alias: {
        '@': path.resolve(__dirname, '../components')
      }
    },
    build: {
      assetsInlineLimit: 0,
    },
    optimizeDeps: {
      exclude: [
        '@nolebase/vitepress-plugin-enhanced-readabilities/client',
        'vitepress',
        '@nolebase/ui',
      ],
    },
    ssr: {
      noExternal: [
        '@nolebase/vitepress-plugin-enhanced-readabilities',
        '@nolebase/ui',
      ],
    },
  },

  themeConfig: {
    outline: 'deep',
    // logo: { src: '/logo_sq.png', width: 24, height: 24 },
    search: {
      provider: 'local',
      options: { detailedView: true }
    },
    nav,
    sidebar,
    editLink: 'REPLACE_ME_DOCUMENTER_VITEPRESS',
    socialLinks: [
      { icon: "github", link: "https://github.com/JuliaIO/Zarr.jl" },
    ],
    footer: {
      message:
        'Made with <a href="https://luxdl.github.io/DocumenterVitepress.jl/stable" target="_blank"><strong>DocumenterVitepress.jl</strong></a><br>Released under the MIT License. Powered by the <a href="https://www.julialang.org">Julia Programming Language</a>.<br>',
    },
  }
})