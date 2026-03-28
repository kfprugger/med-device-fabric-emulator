import React from 'react';
import {
	AbsoluteFill,
	interpolate,
	spring,
	useCurrentFrame,
	useVideoConfig,
} from 'remotion';

export const ClosingSlide: React.FC = () => {
	const frame = useCurrentFrame();
	const {fps} = useVideoConfig();

	const progress = spring({fps, frame, config: {damping: 100}});
	const y = interpolate(progress, [0, 1], [50, 0]);
	const opacity = interpolate(frame, [0, 20], [0, 1], {
		extrapolateRight: 'clamp',
	});

	const cmdOpacity = interpolate(frame, [40, 60], [0, 1], {
		extrapolateRight: 'clamp',
	});

	const statsOpacity = interpolate(frame, [70, 90], [0, 1], {
		extrapolateRight: 'clamp',
	});

	const stats = [
		{value: '10K', label: 'FHIR Patients'},
		{value: '5M+', label: 'Clinical Resources'},
		{value: '100', label: 'Masimo Devices'},
		{value: '3', label: 'AI Data Agents'},
		{value: '6', label: 'Fabric Workloads'},
		{value: '<2hr', label: 'Full Deploy'},
	];

	return (
		<AbsoluteFill
			style={{
				background: 'linear-gradient(135deg, #0a0a2e 0%, #1a1a4e 40%, #0078d4 100%)',
				justifyContent: 'center',
				alignItems: 'center',
			}}
		>
			<div
				style={{
					display: 'flex',
					flexDirection: 'column',
					alignItems: 'center',
					gap: 30,
					transform: `translateY(${y}px)`,
				}}
			>
				<h1
					style={{
						fontSize: 64,
						fontWeight: 800,
						color: 'white',
						fontFamily: 'system-ui, sans-serif',
						textAlign: 'center',
						margin: 0,
						opacity,
					}}
				>
					Get Started
				</h1>

				<div
					style={{
						opacity: cmdOpacity,
						background: 'rgba(0,0,0,0.4)',
						borderRadius: 12,
						padding: '24px 36px',
						border: '1px solid rgba(255,255,255,0.15)',
						maxWidth: 900,
					}}
				>
					<pre
						style={{
							fontSize: 19,
							color: '#00c4b4',
							fontFamily: 'Consolas, monospace',
							margin: 0,
							lineHeight: 1.6,
							whiteSpace: 'pre',
						}}
					>
{`.\\Deploy-All.ps1 \`
    -ResourceGroupName "rg-medtech-rti-fhir" \`
    -Location "eastus" \`
    -FabricWorkspaceName "med-device-rti-hds" \`
    -AdminSecurityGroup "sg-azure-admins" \`
    -PatientCount 100 \`
    -Tags @{SecurityControl='Ignore'}`}
					</pre>
				</div>

				{/* Stats */}
				<div
					style={{
						display: 'flex',
						gap: 40,
						marginTop: 20,
						opacity: statsOpacity,
					}}
				>
					{stats.map((s) => (
						<div
							key={s.label}
							style={{
								display: 'flex',
								flexDirection: 'column',
								alignItems: 'center',
								gap: 4,
							}}
						>
							<span
								style={{
									fontSize: 48,
									fontWeight: 800,
									color: 'white',
									fontFamily: 'system-ui, sans-serif',
								}}
							>
								{s.value}
							</span>
							<span
								style={{
									fontSize: 16,
									color: 'rgba(255,255,255,0.6)',
									fontFamily: 'system-ui, sans-serif',
								}}
							>
								{s.label}
							</span>
						</div>
					))}
				</div>

				<div
					style={{
						marginTop: 30,
						opacity: statsOpacity,
						display: 'flex',
						gap: 16,
					}}
				>
					{['Phase 1: Infrastructure', 'Phase 2: Enrichment', 'Phase 3: Imaging'].map(
						(p, i) => (
							<div
								key={p}
								style={{
									padding: '10px 20px',
									borderRadius: 8,
									background: [
										'rgba(255,140,0,0.2)',
										'rgba(0,160,0,0.2)',
										'rgba(212,0,0,0.2)',
									][i],
									border: `1px solid ${['#ff8c00', '#00a000', '#d40000'][i]}60`,
									color: ['#ff8c00', '#00a000', '#d40000'][i],
										fontSize: 20,
									fontWeight: 600,
									fontFamily: 'system-ui, sans-serif',
								}}
							>
								{p}
							</div>
						)
					)}
				</div>
			</div>
		</AbsoluteFill>
	);
};
