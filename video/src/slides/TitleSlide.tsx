import React from 'react';
import {
	AbsoluteFill,
	interpolate,
	spring,
	useCurrentFrame,
	useVideoConfig,
} from 'remotion';

export const TitleSlide: React.FC = () => {
	const frame = useCurrentFrame();
	const {fps} = useVideoConfig();

	const titleY = interpolate(
		spring({fps, frame, config: {damping: 100}}),
		[0, 1],
		[60, 0]
	);
	const titleOpacity = interpolate(frame, [0, 20], [0, 1], {
		extrapolateRight: 'clamp',
	});
	const subtitleOpacity = interpolate(frame, [25, 45], [0, 1], {
		extrapolateRight: 'clamp',
	});
	const badgesOpacity = interpolate(frame, [50, 70], [0, 1], {
		extrapolateRight: 'clamp',
	});
	const lineWidth = interpolate(frame, [15, 50], [0, 600], {
		extrapolateRight: 'clamp',
	});

	return (
		<AbsoluteFill
			style={{
				background: 'linear-gradient(135deg, #0a0a2e 0%, #1a1a4e 40%, #0078d4 100%)',
				justifyContent: 'center',
				alignItems: 'center',
			}}
		>
			{/* Background grid */}
			<div style={{
				position: 'absolute',
				inset: 0,
				backgroundImage: 'linear-gradient(rgba(255,255,255,0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.03) 1px, transparent 1px)',
				backgroundSize: '60px 60px',
			}} />

			{/* Glowing orb */}
			<div style={{
				position: 'absolute',
				width: 500,
				height: 500,
				borderRadius: '50%',
				background: 'radial-gradient(circle, rgba(0,120,212,0.3) 0%, transparent 70%)',
				top: '20%',
				right: '10%',
				filter: 'blur(40px)',
			}} />

			<div style={{
				display: 'flex',
				flexDirection: 'column',
				alignItems: 'center',
				gap: 20,
				transform: `translateY(${titleY}px)`,
			}}>
				{/* Icon row */}
				<div style={{
					fontSize: 60,
					opacity: badgesOpacity,
					display: 'flex',
					gap: 20,
				}}>
					🏥 ⚡ 🩻 🤖
				</div>

				{/* Title */}
				<h1 style={{
					fontSize: 72,
					fontWeight: 800,
					color: 'white',
					opacity: titleOpacity,
					textAlign: 'center',
					fontFamily: 'system-ui, -apple-system, sans-serif',
					margin: 0,
					letterSpacing: -1,
				}}>
					Medical Device FHIR
					<br />
					Integration Platform
				</h1>

				{/* Accent line */}
				<div style={{
					width: lineWidth,
					height: 4,
					background: 'linear-gradient(90deg, #0078d4, #00c4b4)',
					borderRadius: 2,
				}} />

				{/* Subtitle */}
				<p style={{
					fontSize: 28,
					color: 'rgba(255,255,255,0.8)',
					opacity: subtitleOpacity,
					textAlign: 'center',
					fontFamily: 'system-ui, sans-serif',
					maxWidth: 900,
					lineHeight: 1.5,
					margin: 0,
				}}>
					End-to-end reference architecture unifying healthcare EHR data
					<br />
					and real-time medical device telemetry on Microsoft Fabric
				</p>

				{/* Tech badges */}
				<div style={{
					display: 'flex',
					gap: 12,
					marginTop: 20,
					opacity: badgesOpacity,
				}}>
					{['Azure Health Data Services', 'Microsoft Fabric', 'Real-Time Intelligence', 'Data Agents', 'DICOM'].map((badge) => (
						<div key={badge} style={{
							padding: '8px 18px',
							borderRadius: 20,
							background: 'rgba(255,255,255,0.1)',
							border: '1px solid rgba(255,255,255,0.2)',
							color: 'rgba(255,255,255,0.9)',
							fontSize: 16,
							fontFamily: 'system-ui, sans-serif',
						}}>
							{badge}
						</div>
					))}
				</div>
			</div>
		</AbsoluteFill>
	);
};
