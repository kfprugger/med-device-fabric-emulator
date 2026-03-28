import React from 'react';
import {
	AbsoluteFill,
	interpolate,
	spring,
	useCurrentFrame,
	useVideoConfig,
} from 'remotion';

type Bullet = {icon: string; text: string};

export const BulletSlide: React.FC<{title: string; bullets: Bullet[]}> = ({
	title,
	bullets,
}) => {
	const frame = useCurrentFrame();
	const {fps} = useVideoConfig();

	const titleOpacity = interpolate(frame, [0, 15], [0, 1], {
		extrapolateRight: 'clamp',
	});

	return (
		<AbsoluteFill
			style={{
				background: 'linear-gradient(160deg, #0a0a2e 0%, #12123a 100%)',
				padding: '60px 80px',
			}}
		>
			<h2
				style={{
					fontSize: 56,
					fontWeight: 700,
					color: 'white',
					opacity: titleOpacity,
					fontFamily: 'system-ui, sans-serif',
					margin: 0,
					marginBottom: 50,
				}}
			>
				{title}
			</h2>

			<div style={{display: 'flex', flexDirection: 'column', gap: 32}}>
				{bullets.map((b, i) => {
					const delay = 15 + i * 20;
					const progress = spring({
						fps,
						frame: frame - delay,
						config: {damping: 100},
					});
					const opacity = interpolate(frame, [delay, delay + 12], [0, 1], {
						extrapolateLeft: 'clamp',
						extrapolateRight: 'clamp',
					});
					const x = interpolate(progress, [0, 1], [40, 0]);

					return (
						<div
							key={i}
							style={{
								display: 'flex',
								alignItems: 'flex-start',
								gap: 20,
								opacity,
								transform: `translateX(${x}px)`,
							}}
						>
							<span style={{fontSize: 42, lineHeight: 1}}>{b.icon}</span>
							<span
								style={{
									fontSize: 30,
									color: 'rgba(255,255,255,0.9)',
									fontFamily: 'system-ui, sans-serif',
									lineHeight: 1.4,
								}}
							>
								{b.text}
							</span>
						</div>
					);
				})}
			</div>
		</AbsoluteFill>
	);
};
