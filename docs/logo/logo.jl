using Colors
using GLMakie
using GeometryBasics
using Random
# using CairoMakie # don't use this one, the output comes with artifacts. GLMakie works fine.
# CairoMakie.activate!()

function alpha_colorbuffer(scene)
    bg = scene.backgroundcolor[]
    scene.backgroundcolor[] = RGBAf(0, 0, 0, 1)
    b1 = copy(Makie.colorbuffer(scene))
    scene.backgroundcolor[] = RGBAf(1, 1, 1, 1)
    b2 = Makie.colorbuffer(scene)
    scene.backgroundcolor[] = bg
    return map(infer_alphacolor, b1, b2)
end
function infer_alphacolor(rgb1, rgb2)
    rgb1 == rgb2 && return RGBAf(rgb1.r, rgb1.g, rgb1.b, 1)
    c1 = Float64.((rgb1.r, rgb1.g, rgb1.b))
    c2 = Float64.((rgb2.r, rgb2.g, rgb2.b))
    alpha = @. 1 - (c1 - c2) * -1 # ( / (0 - 1))
    meanalpha = clamp(sum(alpha) / 3, 0, 1)
    meanalpha == 0 && return RGBAf(0, 0, 0, 0)
    c = @. clamp((c1 / meanalpha), 0, 1)
    return RGBAf(c..., meanalpha)
end

function make_logo(; is_wireframe::Bool = true, transparency::Bool = false, seed=16151, size = (600,600))
    Random.seed!(seed)
    N = 7

    left_points = [Point3f(0.5, j, k) for j in 1:N for k in 1:N]
    back_points = [Point3f(i, 0.5, k) for i in 1:N for k in 1:N]
    bottom_points = [Point3f(i, j, 0.5) for i in 1:N for j in 1:N]

    colors = ["#e58077", "#e34b75", "#bb1085", "#4063D8", "#389826", "#9558B2", "#00A3E0"]

    rot_angle = 3 * pi / 4
    tilt_angle = -pi / 8 # Tilt Z backwards by 22.5 degrees
    global_rot = Makie.qrotation(Vec3f(0,0,1), Float32(rot_angle)) * Makie.qrotation(Vec3f(1,0,0), Float32(tilt_angle))

    transform_p3(x_l, y_l, z_l) = begin
        x_t = x_l
        y_t = y_l * cos(tilt_angle) - (z_l - 4) * sin(tilt_angle)
        z_t = y_l * sin(tilt_angle) + (z_l - 4) * cos(tilt_angle)
        
        x_rot = x_t * cos(rot_angle) - y_t * sin(rot_angle)
        y_rot = x_t * sin(rot_angle) + y_t * cos(rot_angle)
        
        return Point3f(x_rot + 4, y_rot + 4, z_t + 4)
    end

    z_pts_2d = [
        Point2f(-2.0, 6.4),        # 1
        Point2f(2.0, 6.4),         # 2
        Point2f(2.0, 5.434315),    # 3
        Point2f(-1.034315, 2.4),   # 4
        Point2f(2.0, 2.4),         # 5
        Point2f(2.0, 1.6),         # 6
        Point2f(-2.0, 1.6),        # 7
        Point2f(-2.0, 2.565685),   # 8
        Point2f(1.034315, 5.6),    # 9
        Point2f(-2.0, 5.6)         # 10
    ]

    z_vertices = Point3f[]
    z_colors = Float32[]
    for p in z_pts_2d
        push!(z_vertices, transform_p3(p[1], -0.399, p[2]))
        push!(z_colors, p[2])
    end
    for p in z_pts_2d
        push!(z_vertices, transform_p3(p[1], 0.399, p[2]))
        push!(z_colors, p[2])
    end

    z_faces = GLTriangleFace[]
    push!(z_faces, GLTriangleFace(1, 10, 9), GLTriangleFace(1, 9, 2), GLTriangleFace(2, 9, 3))
    push!(z_faces, GLTriangleFace(9, 8, 3), GLTriangleFace(8, 4, 3), GLTriangleFace(8, 7, 4))
    push!(z_faces, GLTriangleFace(4, 7, 6), GLTriangleFace(4, 6, 5))
    push!(z_faces, GLTriangleFace(11, 19, 20), GLTriangleFace(11, 12, 19), GLTriangleFace(12, 13, 19))
    push!(z_faces, GLTriangleFace(19, 13, 18), GLTriangleFace(18, 13, 14), GLTriangleFace(18, 14, 17))
    push!(z_faces, GLTriangleFace(14, 16, 17), GLTriangleFace(14, 15, 16))
    
    boundary = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1]
    side_start = 20
    for i in 1:10
        u, v = boundary[i], boundary[i+1]
        push!(z_vertices, z_vertices[u], z_vertices[v], z_vertices[v+10], z_vertices[u+10])
        push!(z_colors, z_colors[u], z_colors[v], z_colors[v+10], z_colors[u+10])
        idx = side_start + (i-1)*4
        push!(z_faces, GLTriangleFace(idx+1, idx+3, idx+2), GLTriangleFace(idx+1, idx+4, idx+3))
    end
    fig = Figure(; size=size,) # backgroundcolor=:transparent
    ax = LScene(fig[1,1]; show_axis=false,)
    
    cam = cameracontrols(ax.scene)
    cam.eyeposition[] = Vec3f(14.008474063013997, 14.117945037759231, 9.072624070823204)
    cam.lookat[] = Vec3f(4.25191593170166, 4.300845623016357, 3.3947386741638184)
    cam.upvector[] = Vec3f(-0.26754081581622086, -0.269200937087842, 0.9251771939077157)
    cam.fov[] = 45.0
    update_cam!(ax.scene, cam)

    set_ambient_light!(ax, RGBf(0.7, 0.7, 0.7))
    set_lights!(ax, [DirectionalLight(RGBf(0.15, 0.15, 0.15), cam.lookat[] - cam.eyeposition[])])
    
    # Walls
    wall_c1 = colorant"#2E3440"
    wall_c2 = colorant"#E5E7EB"
    left_colors = [rand((wall_c1, wall_c2)) for j in 1:N for k in 1:N]
    back_colors = [rand((wall_c1, wall_c2)) for i in 1:N for k in 1:N]
    bottom_colors = [rand((wall_c1, wall_c2)) for i in 1:N for j in 1:N]

    accent_colors = [colorant"#4063D8", colorant"#9558B2", colorant"#389826", colorant"#CB3C33"]
    corner_indices = [1, N, (N-1)*N + 1, N^2]
    for (i, idx) in enumerate(corner_indices)
        if idx != 1
            left_colors[idx] = accent_colors[i]
            back_colors[idx] = accent_colors[mod1(i+1, 4)]
            bottom_colors[idx] = accent_colors[mod1(i+2, 4)]
        end
    end

    if is_wireframe
        wall_alpha(p) = clamp(0.05f0 + 0.95f0 * max(0.0f0, (p[1] - 1.0f0)^2 + (p[2] - 1.0f0)^2 + (p[3] - 1.0f0)^2 - 0.25f0) / 72.0f0, 0.05f0, 1.0f0)
        left_wf_marker = Rect3f(Vec3f(-0.025, -0.49, -0.49), Vec3f(0.05, 0.98, 0.98))
        left_solid_marker = Rect3f(Vec3f(-0.025, -0.49, -0.49), Vec3f(0.05, 0.98, 0.98))
        left_acc_pts = [p for (p, c) in zip(left_points, left_colors) if c in accent_colors]
        left_acc_cls = [c for (p, c) in zip(left_points, left_colors) if c in accent_colors]
        if !isempty(left_acc_pts)
            meshscatter!(ax, left_acc_pts; marker=left_solid_marker, markersize=1f0, color=left_acc_cls, transparency=false, shading=true)
        end
        for (p, c) in zip(left_points, left_colors)
            alpha_val = wall_alpha(p) + 0.1f0
            c_alpha = RGBAf(c.r, c.g, c.b, alpha_val)
            wireframe!(ax, Rect3f(p .+ left_wf_marker.origin, left_wf_marker.widths); color=c_alpha, linewidth=1.5f0 * alpha_val, transparency=true)
        end

        back_wf_marker = Rect3f(Vec3f(-0.49, -0.025, -0.49), Vec3f(0.98, 0.05, 0.98))
        back_solid_marker = Rect3f(Vec3f(-0.49, -0.025, -0.49), Vec3f(0.98, 0.05, 0.98))
        back_acc_pts = [p for (p, c) in zip(back_points, back_colors) if c in accent_colors]
        back_acc_cls = [c for (p, c) in zip(back_points, back_colors) if c in accent_colors]
        if !isempty(back_acc_pts)
            meshscatter!(ax, back_acc_pts; marker=back_solid_marker, markersize=1f0, color=back_acc_cls, transparency=false, shading=true)
        end
        for (p, c) in zip(back_points, back_colors)
            alpha_val = wall_alpha(p) + 0.1f0
            c_alpha = RGBAf(c.r, c.g, c.b, alpha_val)
            wireframe!(ax, Rect3f(p .+ back_wf_marker.origin, back_wf_marker.widths); color=c_alpha, linewidth=1.5f0 * alpha_val, transparency=true)
        end

        bot_wf_marker = Rect3f(Vec3f(-0.49, -0.49, -0.025), Vec3f(0.98, 0.98, 0.05))
        bot_solid_marker = Rect3f(Vec3f(-0.49, -0.49, -0.025), Vec3f(0.98, 0.98, 0.05))
        bot_acc_pts = [p for (p, c) in zip(bottom_points, bottom_colors) if c in accent_colors]
        bot_acc_cls = [c for (p, c) in zip(bottom_points, bottom_colors) if c in accent_colors]
        if !isempty(bot_acc_pts)
            meshscatter!(ax, bot_acc_pts; marker=bot_solid_marker, markersize=1f0, color=bot_acc_cls, transparency=false, shading=true)
        end
        for (p, c) in zip(bottom_points, bottom_colors)
            alpha_val = wall_alpha(p) + 0.1f0
            c_alpha = RGBAf(c.r, c.g, c.b, alpha_val)
            wireframe!(ax, Rect3f(p .+ bot_wf_marker.origin, bot_wf_marker.widths); color=c_alpha, linewidth=1.5f0 * alpha_val, transparency=true)
        end
    else
        meshscatter!(ax, left_points; marker=Rect3f(Vec3f(-0.1, -0.49, -0.49), Vec3f(0.2, 0.98, 0.98)), markersize=1f0, color=left_colors, transparency=transparency, shading=true)
        meshscatter!(ax, back_points; marker=Rect3f(Vec3f(-0.49, -0.1, -0.49), Vec3f(0.98, 0.2, 0.98)), markersize=1f0, color=back_colors, transparency=transparency, shading=true)
        meshscatter!(ax, bottom_points; marker=Rect3f(Vec3f(-0.49, -0.49, -0.1), Vec3f(0.98, 0.98, 0.2)), markersize=1f0, color=bottom_colors, transparency=transparency, shading=true)
    end

    # Z shape
    z_cmap = tuple.(reverse(colors[1:3]), 1)
    z_mesh = GeometryBasics.normal_mesh(z_vertices, z_faces)
    mesh!(ax, z_mesh; color=z_colors, colorrange=(2, 6), colormap=z_cmap, transparency=false, shading=true, shininess=128f0, specular=Vec3f(1.0, 1.0, 1.0))
    return fig, ax
end

#wireframe
# with_theme(theme_dark()) do 
fig, ax = make_logo(is_wireframe=true, transparency=true)
display(fig, update=false) 
# end

mkpath(joinpath(@__DIR__, "../src", "public"))
save(joinpath(@__DIR__, "../src", "public", "logo.png"), alpha_colorbuffer(fig.scene))

# favicon
fig_fav, ax_fav = make_logo(is_wireframe=true, transparency=true, size=(300,300))
save(joinpath(@__DIR__, "../src", "public", "favicon.png"), alpha_colorbuffer(fig_fav.scene))
mv(joinpath(@__DIR__, "../src", "public", "favicon.png"), joinpath(@__DIR__, "../src", "public", "favicon.ico"), force=true)

# solid
fig_solid, ax_solid = make_logo(is_wireframe=false, transparency=false)
# display(fig, update=false)

# mkpath(joinpath(@__DIR__, "../src", "public"))
save(joinpath(@__DIR__, "../src", "public", "logo_solid.png"), alpha_colorbuffer(fig_solid.scene))

# favicon solid
fig_fav_solid, ax_fav_solid = make_logo(is_wireframe=false, transparency=false, size=(300,300))
save(joinpath(@__DIR__, "../src", "public", "favicon_solid.png"), alpha_colorbuffer(fig_fav_solid.scene))
mv(joinpath(@__DIR__, "../src", "public", "favicon_solid.png"), joinpath(@__DIR__, "../src", "public", "favicon_solid.ico"), force=true)