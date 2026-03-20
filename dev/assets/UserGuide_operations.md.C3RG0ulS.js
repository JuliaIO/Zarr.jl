import{_ as a,o as n,c as e,aB as i}from"./chunks/framework.Xgz8C-wX.js";const u=JSON.parse('{"title":"","description":"","frontmatter":{},"headers":[],"relativePath":"UserGuide/operations.md","filePath":"UserGuide/operations.md","lastUpdated":null}'),p={name:"UserGuide/operations.md"};function t(l,s,o,r,c,d){return n(),e("div",null,[...s[0]||(s[0]=[i(`<h2 id="Operations-on-Zarr-Arrays" tabindex="-1">Operations on Zarr Arrays <a class="header-anchor" href="#Operations-on-Zarr-Arrays" aria-label="Permalink to &quot;Operations on Zarr Arrays {#Operations-on-Zarr-Arrays}&quot;">​</a></h2><p>A Zarr Array consists of a collection of potentially compressed chunks, and there is a significant overhead in accessing a single item from such an array compared to Julia&#39;s Base Array type.</p><p>In order to make operations on <code>ZArray</code>s still efficient, we use the <a href="https://github.com/meggart/DiskArrays.jl/" target="_blank" rel="noreferrer">DiskArrays</a> package which enables efficient broadcast and reductions on <code>Zarray</code>s respecting their chunk sizes. This includes some modified behavior compared to a normal <code>AbstractArray</code>, including lazy broadcasting and a non-default array access order for reductions.</p><p>Please refer to the DiskArrays documentation to see which operations are supported.</p><h3 id="A-short-example" tabindex="-1">A short example <a class="header-anchor" href="#A-short-example" aria-label="Permalink to &quot;A short example {#A-short-example}&quot;">​</a></h3><div class="language-@jldoctest vp-adaptive-theme"><button title="Copy Code" class="copy"></button><span class="lang">@jldoctest</span><pre class="shiki shiki-themes github-light github-dark vp-code" tabindex="0"><code><span class="line"><span>julia&gt; using Zarr, Statistics</span></span>
<span class="line"><span></span></span>
<span class="line"><span>julia&gt; g = zopen(&quot;gs://cmip6/CMIP/NCAR/CESM2/historical/r9i1p1f1/Amon/tas/gn/&quot;, consolidated=true)</span></span>
<span class="line"><span>ZarrGroup at Consolidated S3 Object Storage</span></span>
<span class="line"><span>Variables: lat time tas lat_bnds lon_bnds lon time_bnds</span></span></code></pre></div><p>Accessing a single element from the array has significant overhead, because a whole chunk has to be transferred from GCS and unzipped:</p><div class="language-julia vp-adaptive-theme"><button title="Copy Code" class="copy"></button><span class="lang">julia</span><pre class="shiki shiki-themes github-light github-dark vp-code" tabindex="0"><code><span class="line"><span style="--shiki-light:#24292E;--shiki-dark:#E1E4E8;">julia</span><span style="--shiki-light:#D73A49;--shiki-dark:#F97583;">&gt;</span><span style="--shiki-light:#005CC5;--shiki-dark:#79B8FF;"> @time</span><span style="--shiki-light:#24292E;--shiki-dark:#E1E4E8;"> g[</span><span style="--shiki-light:#032F62;--shiki-dark:#9ECBFF;">&quot;tas&quot;</span><span style="--shiki-light:#24292E;--shiki-dark:#E1E4E8;">][</span><span style="--shiki-light:#005CC5;--shiki-dark:#79B8FF;">1</span><span style="--shiki-light:#24292E;--shiki-dark:#E1E4E8;">,</span><span style="--shiki-light:#005CC5;--shiki-dark:#79B8FF;">1</span><span style="--shiki-light:#24292E;--shiki-dark:#E1E4E8;">,</span><span style="--shiki-light:#005CC5;--shiki-dark:#79B8FF;">1</span><span style="--shiki-light:#24292E;--shiki-dark:#E1E4E8;">]</span></span></code></pre></div><div class="language- vp-adaptive-theme"><button title="Copy Code" class="copy"></button><span class="lang"></span><pre class="shiki shiki-themes github-light github-dark vp-code" tabindex="0"><code><span class="line"><span>18.734581 seconds (129.25 k allocations: 557.614 MiB, 0.56% gc time)</span></span>
<span class="line"><span></span></span>
<span class="line"><span>244.39726f0</span></span></code></pre></div><div class="language-@jldoctest vp-adaptive-theme"><button title="Copy Code" class="copy"></button><span class="lang">@jldoctest</span><pre class="shiki shiki-themes github-light github-dark vp-code" tabindex="0"><code><span class="line"><span>julia&gt; latweights = reshape(cosd.(g[&quot;lat&quot;])[:],1,192,1);</span></span>
<span class="line"><span></span></span>
<span class="line"><span>julia&gt; t_celsius = g[&quot;tas&quot;].-273.15</span></span>
<span class="line"><span>Disk Array with size 288 x 192 x 1980</span></span>
<span class="line"><span></span></span>
<span class="line"><span>julia&gt; t_w = t_celsius .* latweights</span></span>
<span class="line"><span>Disk Array with size 288 x 192 x 1980</span></span></code></pre></div><p>Note that the broadcast operations are not directly computed but are collected in a fused lazy Broadcast object. When calling a reducing operation on the array, it will be read chunk by chunk and means will be merged instead of accessing the elements in a naive loop, so that the computation can be finished in reasonable time:</p><div class="language-@jldoctest vp-adaptive-theme"><button title="Copy Code" class="copy"></button><span class="lang">@jldoctest</span><pre class="shiki shiki-themes github-light github-dark vp-code" tabindex="0"><code><span class="line"><span>julia&gt; mean(t_w, dims = (1,2))./mean(latweights)</span></span>
<span class="line"><span>1×1×1980 Array{Float64,3}:</span></span>
<span class="line"><span>[:, :, 1] =</span></span>
<span class="line"><span> 12.492234157689309</span></span>
<span class="line"><span></span></span>
<span class="line"><span>[:, :, 2] =</span></span>
<span class="line"><span> 12.425466417315654</span></span>
<span class="line"><span></span></span>
<span class="line"><span>[:, :, 3] =</span></span>
<span class="line"><span> 13.190267552582446</span></span>
<span class="line"><span></span></span>
<span class="line"><span>...</span></span>
<span class="line"><span></span></span>
<span class="line"><span>[:, :, 1978] =</span></span>
<span class="line"><span> 15.55063620093181</span></span>
<span class="line"><span></span></span>
<span class="line"><span>[:, :, 1979] =</span></span>
<span class="line"><span> 14.614388350826788</span></span>
<span class="line"><span></span></span>
<span class="line"><span>[:, :, 1980] =</span></span>
<span class="line"><span> 13.913361540597469</span></span></code></pre></div>`,12)])])}const g=a(p,[["render",t]]);export{u as __pageData,g as default};
