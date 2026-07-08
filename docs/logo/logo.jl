using Colors
using GLMakie
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

function make_logo(; is_wireframe::Bool = true, transparency::Bool = false, seed=161, size = (600,600))
    Random.seed!(seed)
    N = 7

    left_points = [Point3f(0.5, j, k) for j in 1:N for k in 1:N]
    back_points = [Point3f(i, 0.5, k) for i in 1:N for k in 1:N]
    bottom_points = [Point3f(i, j, 0.5) for i in 1:N for j in 1:N]

    colors = ["#e58077", "#e34b75", "#bb1085", "#4063D8", "#389826", "#9558B2", "#00A3E0"]

    rot_angle = 3 * pi / 4
    tilt_angle = -pi / 8 # Tilt Z backwards by 22.5 degrees
    global_rot = Makie.qrotation(Vec3f(0,0,1), Float32(rot_angle)) * Makie.qrotation(Vec3f(1,0,0), Float32(tilt_angle))

    transform_p(x_c, z_c) = begin
        x_t = x_c
        y_t = - (z_c - 4) * sin(tilt_angle)
        z_t = (z_c - 4) * cos(tilt_angle)
        
        x_rot = x_t * cos(rot_angle) - y_t * sin(rot_angle)
        y_rot = x_t * sin(rot_angle) + y_t * cos(rot_angle)
        
        return Point3f(x_rot + 4, y_rot + 4, z_t + 4)
    end

    z_top_points = Point3f[]
    z_bottom_points = Point3f[]
    z_diag_points = Point3f[]
    z_top_colors = Float32[]
    z_bottom_colors = Float32[]
    z_diag_colors = Float32[]

    for x_c in -2:0.0025:2
        push!(z_top_points, transform_p(x_c, 6))
        push!(z_top_colors, 6f0)
        
        push!(z_bottom_points, transform_p(x_c, 2))
        push!(z_bottom_colors, 2f0)
    end

    z_diag_markersizes = Vec3f[]
    for x_c in -2:0.0025:2
        z_c = x_c + 4
        
        target_Z_top = z_c + 0.8 * sqrt(2) / 2
        target_Z_bot = z_c - 0.8 * sqrt(2) / 2
        
        actual_Z_top = min(target_Z_top, 6.399)
        actual_Z_bot = max(target_Z_bot, 1.601)
        
        actual_H = actual_Z_top - actual_Z_bot
        actual_z_c = (actual_Z_top + actual_Z_bot) / 2
        
        push!(z_diag_points, transform_p(x_c, actual_z_c))
        push!(z_diag_markersizes, Vec3f(0.0025, 0.8, actual_H))
        push!(z_diag_colors, Float32(actual_z_c))
    end

    fig_out = with_theme(theme_light()) do
        fig = Figure(; size=size,) # backgroundcolor=:transparent
        ax = LScene(fig[1,1]; show_axis=false,)
        
        cam = cameracontrols(ax.scene)
        cam.eyeposition[] = Vec3f(12.88912707708998, 12.938056650658535, 12.03194959663817)
        cam.lookat[] = Vec3f(4.251916045255032, 4.300845618823581, 3.394738564803229)
        cam.upvector[] = Vec3f(-0.40824829046386296, -0.40824829046386296, 0.8164965809277259)
        cam.fov[] = 45.0
        update_cam!(ax.scene, cam)
        # Walls
        wall_c1 = colorant"#2E3440"
        wall_c2 = colorant"#E5E7EB"
        left_colors = [rand((wall_c1, wall_c2)) for j in 1:N for k in 1:N]
        back_colors = [rand((wall_c1, wall_c2)) for i in 1:N for k in 1:N]
        bottom_colors = [rand((wall_c1, wall_c2)) for i in 1:N for j in 1:N]

        accent_colors = [colorant"#9558B2", colorant"#389826", colorant"#CB3C33", colorant"#4063D8"]
        for wall in (left_colors, back_colors, bottom_colors)
            num_accents = rand(3:5)
            indices = unique(rand(1:(N^2), 15))[1:num_accents]
            for idx in indices
                wall[idx] = rand(accent_colors)
            end
        end

        if is_wireframe
            left_marker = Rect3f(Vec3f(-0.025, -0.49, -0.49), Vec3f(0.05, 0.98, 0.98))
            for (p, c) in zip(left_points, left_colors)
                wireframe!(ax, Rect3f(p .+ left_marker.origin, left_marker.widths); color=c, linewidth=1.5, transparency=transparency)
            end

            back_marker = Rect3f(Vec3f(-0.49, -0.025, -0.49), Vec3f(0.98, 0.05, 0.98))
            for (p, c) in zip(back_points, back_colors)
                wireframe!(ax, Rect3f(p .+ back_marker.origin, back_marker.widths); color=c, linewidth=1.5, transparency=transparency)
            end

            bottom_marker = Rect3f(Vec3f(-0.49, -0.49, -0.025), Vec3f(0.98, 0.98, 0.05))
            for (p, c) in zip(bottom_points, bottom_colors)
                wireframe!(ax, Rect3f(p .+ bottom_marker.origin, bottom_marker.widths); color=c, linewidth=1.5, transparency=transparency)
            end
        else
            meshscatter!(ax, left_points; marker=Rect3f(Vec3f(-0.1, -0.49, -0.49), Vec3f(0.2, 0.98, 0.98)), markersize=1f0, color=left_colors, transparency=transparency, shading=true)
            meshscatter!(ax, back_points; marker=Rect3f(Vec3f(-0.49, -0.1, -0.49), Vec3f(0.98, 0.2, 0.98)), markersize=1f0, color=back_colors, transparency=transparency, shading=true)
            meshscatter!(ax, bottom_points; marker=Rect3f(Vec3f(-0.49, -0.49, -0.1), Vec3f(0.98, 0.98, 0.2)), markersize=1f0, color=bottom_colors, transparency=transparency, shading=true)
        end

        # Z shape
        base_marker = Rect3f(Vec3f(-0.5, -0.5, -0.5), Vec3f(1.0, 1.0, 1.0))
        z_cmap = tuple.(reverse(colors[1:3]), 1)

        meshscatter!(ax, z_top_points; marker=base_marker, markersize=Vec3f(0.0025, 0.798, 0.798),
            rotation=global_rot, color=z_top_colors, colorrange=(2, 6), colormap=z_cmap, transparency=false)
        meshscatter!(ax, z_bottom_points; marker=base_marker, markersize=Vec3f(0.0025, 0.798, 0.798),
            rotation=global_rot, color=z_bottom_colors, colorrange=(2, 6), colormap=z_cmap, transparency=false)
        meshscatter!(ax, z_diag_points; marker=base_marker, markersize=z_diag_markersizes,
            rotation=global_rot, color=z_diag_colors, colorrange=(2, 6), colormap=z_cmap, transparency=false)
        
        return fig, ax
    end
    return fig_out
end

#wireframe
fig, ax = make_logo(is_wireframe=true, transparency=true)
# display(fig, update=false)

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