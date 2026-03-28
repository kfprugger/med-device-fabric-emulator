import React from 'react';
import {
	AbsoluteFill,
	interpolate,
	spring,
	useCurrentFrame,
	useVideoConfig,
} from 'remotion';

const agents = [
	{
		name: 'Patient 360',
		icon: '🧑‍⚕️',
		desc: 'Unified patient view across FHIR clinical data and real-time telemetry',
		queries: [
			'"Show patient info for device MASIMO-RADIUS7-0033"',
			'"Latest vitals and conditions for patient X"',
		],
		sources: ['KQL: TelemetryRaw + AlertHistory', 'SQL: dbo.Patient, dbo.Condition, dbo.Basic'],
	},
	{
		name: 'Clinical Triage',
		icon: '🚨',
		desc: 'Rapid triage decisions with multi-metric alert detection and severity tiers',
		queries: [
			'"Run a clinical triage"',
			'"Which devices have low SpO2? Look up the patients."',
		],
		sources: ['KQL: Real-time telemetry', 'SQL: Patient demographics + conditions'],
	},
	{
		name: 'Cohorting Agent',
		icon: '🔬',
		desc: 'Natural language queries against Gold OMOP CDM tables for imaging cohorts',
		queries: [
			'"Find all COPD patients with chest CTs in the last 6 months"',
			'"How many diabetic patients have imaging studies?"',
		],
		sources: ['SQL: Gold OMOP CDM v5.4 tables'],
	},
];

export const AgentsSlide: React.FC = () => {
	const frame = useCurrentFrame();
	const {fps} = useVideoConfig();

	return (
		<AbsoluteFill
			style={{
				background: 'linear-gradient(160deg, #0a0a2e 0%, #12123a 100%)',
				padding: '50px 60px',
			}}
		>
			<h2
				style={{
					fontSize: 52,
					fontWeight: 700,
					color: 'white',
					fontFamily: 'system-ui, sans-serif',
					margin: 0,
					marginBottom: 30,
					opacity: interpolate(frame, [0, 15], [0, 1], {
						extrapolateRight: 'clamp',
					}),
				}}
			>
				🤖 AI Data Agents
			</h2>

			<div style={{display: 'flex', gap: 24, flex: 1}}>
				{agents.map((agent, i) => {
					const delay = 15 + i * 30;
					const progress = spring({
						fps,
						frame: frame - delay,
						config: {damping: 80},
					});
					const opacity = interpolate(progress, [0, 1], [0, 1]);
					const y = interpolate(progress, [0, 1], [40, 0]);

					return (
						<div
							key={i}
							style={{
								flex: 1,
								background: 'rgba(255,255,255,0.05)',
								border: '1px solid rgba(255,255,255,0.15)',
								borderRadius: 16,
								padding: '32px 28px',
								opacity,
								transform: `translateY(${y}px)`,
								display: 'flex',
								flexDirection: 'column',
								gap: 18,
							}}
						>
							<div style={{display: 'flex', alignItems: 'center', gap: 14}}>
								<span style={{fontSize: 44}}>{agent.icon}</span>
								<span
									style={{
										fontSize: 30,
										fontWeight: 700,
										color: 'white',
										fontFamily: 'system-ui, sans-serif',
									}}
								>
									{agent.name}
								</span>
							</div>
							<p
								style={{
									fontSize: 21,
									color: 'rgba(255,255,255,0.75)',
									fontFamily: 'system-ui, sans-serif',
									margin: 0,
									lineHeight: 1.5,
								}}
							>
								{agent.desc}
							</p>
							<div style={{marginTop: 10, display: 'flex', flexDirection: 'column', gap: 10}}>
								{agent.queries.map((q, qi) => (
									<div
										key={qi}
										style={{
											fontSize: 18,
											color: '#00c4b4',
											fontFamily: 'monospace',
											background: 'rgba(0,200,180,0.1)',
											padding: '10px 14px',
											borderRadius: 8,
											lineHeight: 1.4,
										}}
									>
										{q}
									</div>
								))}
							</div>
							<div style={{fontSize: 16, color: 'rgba(255,255,255,0.45)', fontFamily: 'system-ui, sans-serif', marginTop: 'auto', lineHeight: 1.4}}>
								{agent.sources.join(' · ')}
							</div>
						</div>
					);
				})}
			</div>
		</AbsoluteFill>
	);
};
