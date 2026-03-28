import React from 'react';
import {
	AbsoluteFill,
	interpolate,
	spring,
	useCurrentFrame,
	useVideoConfig,
} from 'remotion';

const steps = [
	{step: '1', label: 'Synthea generates FHIR R4 bundles', icon: '🧬'},
	{step: '2', label: 'Bundles saved to Azure Blob Storage', icon: '📦'},
	{step: '3', label: 'FHIR Loader transforms + uploads to FHIR Service', icon: '🔄'},
	{step: '4', label: 'Device associations link Masimo devices to patients', icon: '🔗'},
	{step: '5', label: 'DICOM Loader downloads, re-tags, uploads TCIA studies', icon: '🩻'},
	{step: '6', label: 'OneLake shortcuts connect ADLS Gen2 → Bronze Lakehouse', icon: '🔗'},
	{step: '7', label: 'HDS pipelines flow data through Bronze → Silver → Gold', icon: '🏗️'},
	{step: '8', label: 'KQL external tables bridge Silver ↔ Eventhouse', icon: '⚡'},
];

export const DataFlowSlide: React.FC = () => {
	const frame = useCurrentFrame();
	const {fps} = useVideoConfig();

	return (
		<AbsoluteFill
			style={{
				background: 'linear-gradient(160deg, #0a0a2e 0%, #12123a 100%)',
				padding: '70px 100px',
			}}
		>
			<h2
				style={{
					fontSize: 46,
					fontWeight: 700,
					color: 'white',
					fontFamily: 'system-ui, sans-serif',
					margin: 0,
					marginBottom: 40,
					opacity: interpolate(frame, [0, 15], [0, 1], {
						extrapolateRight: 'clamp',
					}),
				}}
			>
				Data Flow Pipeline
			</h2>

			<div
				style={{
					display: 'grid',
					gridTemplateColumns: '1fr 1fr',
					gap: '20px 60px',
				}}
			>
				{steps.map((s, i) => {
					const delay = 10 + i * 15;
					const progress = spring({
						fps,
						frame: frame - delay,
						config: {damping: 80},
					});
					const opacity = interpolate(progress, [0, 1], [0, 1]);
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
									width: 44,
									height: 44,
									borderRadius: 10,
									background: 'rgba(0,120,212,0.2)',
									border: '2px solid rgba(0,120,212,0.4)',
									display: 'flex',
									alignItems: 'center',
									justifyContent: 'center',
									fontSize: 18,
									fontWeight: 800,
									color: '#0078d4',
									fontFamily: 'system-ui, sans-serif',
									flexShrink: 0,
								}}
							>
								{s.step}
							</div>
							<div style={{display: 'flex', alignItems: 'center', gap: 10}}>
								<span style={{fontSize: 28}}>{s.icon}</span>
								<span
									style={{
										fontSize: 22,
										color: 'rgba(255,255,255,0.9)',
										fontFamily: 'system-ui, sans-serif',
										lineHeight: 1.3,
									}}
								>
									{s.label}
								</span>
							</div>
						</div>
					);
				})}
			</div>
		</AbsoluteFill>
	);
};
