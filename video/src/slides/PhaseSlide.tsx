import React from 'react';
import {
	AbsoluteFill,
	interpolate,
	spring,
	useCurrentFrame,
	useVideoConfig,
} from 'remotion';

export const PhaseSlide: React.FC<{
	phase: number;
	title: string;
	color: string;
	duration: string;
	items: string[];
}> = ({phase, title, color, duration, items}) => {
	const frame = useCurrentFrame();
	const {fps} = useVideoConfig();

	const headerProgress = spring({fps, frame, config: {damping: 100}});
	const headerX = interpolate(headerProgress, [0, 1], [-50, 0]);

	return (
		<AbsoluteFill
			style={{
				background: 'linear-gradient(160deg, #0a0a2e 0%, #12123a 100%)',
				padding: '60px 80px',
			}}
		>
			{/* Phase number accent */}
			<div
				style={{
					position: 'absolute',
					right: 80,
					top: 60,
					fontSize: 200,
					fontWeight: 900,
					color: `${color}18`,
					fontFamily: 'system-ui, sans-serif',
					lineHeight: 1,
				}}
			>
				{phase}
			</div>

			{/* Header */}
			<div
				style={{
					display: 'flex',
					alignItems: 'center',
					gap: 20,
					marginBottom: 16,
					transform: `translateX(${headerX}px)`,
				}}
			>
				<div
					style={{
						width: 50,
						height: 50,
						borderRadius: 12,
						background: color,
						display: 'flex',
						alignItems: 'center',
						justifyContent: 'center',
						fontSize: 24,
						fontWeight: 800,
						color: 'white',
						fontFamily: 'system-ui, sans-serif',
					}}
				>
					{phase}
				</div>
				<h2
					style={{
						fontSize: 54,
						fontWeight: 700,
						color: 'white',
						fontFamily: 'system-ui, sans-serif',
						margin: 0,
					}}
				>
					{title}
				</h2>
			</div>

			<div
				style={{
					fontSize: 20,
					color: 'rgba(255,255,255,0.5)',
					fontFamily: 'system-ui, sans-serif',
					marginBottom: 40,
					opacity: interpolate(frame, [10, 25], [0, 1], {
						extrapolateRight: 'clamp',
					}),
				}}
			>
				Typical duration: {duration}
			</div>

			{/* Items */}
			<div style={{display: 'flex', flexDirection: 'column', gap: 28}}>
				{items.map((item, i) => {
					const delay = 20 + i * 18;
					const progress = spring({
						fps,
						frame: frame - delay,
						config: {damping: 100},
					});
					const opacity = interpolate(frame, [delay, delay + 10], [0, 1], {
						extrapolateLeft: 'clamp',
						extrapolateRight: 'clamp',
					});
					const x = interpolate(progress, [0, 1], [30, 0]);

					return (
						<div
							key={i}
							style={{
								display: 'flex',
								alignItems: 'center',
								gap: 16,
								opacity,
								transform: `translateX(${x}px)`,
							}}
						>
							<div
								style={{
									width: 10,
									height: 10,
									borderRadius: '50%',
									background: color,
									flexShrink: 0,
								}}
							/>
							<span
								style={{
									fontSize: 30,
									color: 'rgba(255,255,255,0.9)',
									fontFamily: 'system-ui, sans-serif',
								}}
							>
								{item}
							</span>
						</div>
					);
				})}
			</div>
		</AbsoluteFill>
	);
};
