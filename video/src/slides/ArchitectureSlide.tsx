import React from 'react';
import {
	AbsoluteFill,
	interpolate,
	spring,
	useCurrentFrame,
	useVideoConfig,
} from 'remotion';

const boxes = [
	// Azure group — left half, spread to fill vertically
	{label: 'Synthea\n10K Patients', x: 50, y: 50, w: 210, h: 85, color: '#ff8c00', group: 'azure'},
	{label: 'FHIR R4\nService', x: 300, y: 50, w: 210, h: 85, color: '#d40000', group: 'azure'},
	{label: 'ADLS Gen2\n$export + DICOM', x: 550, y: 50, w: 230, h: 85, color: '#0078d4', group: 'azure'},
	{label: 'Event Hub\nTelemetry', x: 50, y: 200, w: 210, h: 85, color: '#0050a0', group: 'azure'},
	{label: 'DICOM\nLoader', x: 300, y: 200, w: 210, h: 85, color: '#00a060', group: 'azure'},
	{label: 'ACR + Key Vault\n+ Managed Identity', x: 550, y: 200, w: 230, h: 85, color: '#8000d4', group: 'azure'},
	{label: 'Masimo\nEmulator', x: 50, y: 350, w: 210, h: 85, color: '#0050a0', group: 'azure'},
	{label: 'DICOM Service\n(HDS Workspace)', x: 300, y: 350, w: 210, h: 85, color: '#00a060', group: 'azure'},
	{label: 'TCIA\n(Public DICOM)', x: 550, y: 350, w: 230, h: 85, color: '#666', group: 'azure'},

	// Fabric group — right half, spread to fill vertically
	{label: 'Bronze\nLakehouse', x: 880, y: 50, w: 190, h: 80, color: '#f57f17', group: 'fabric'},
	{label: 'Silver\nLakehouse', x: 1100, y: 50, w: 190, h: 80, color: '#1565c0', group: 'fabric'},
	{label: 'Gold OMOP\nLakehouse', x: 880, y: 170, w: 190, h: 80, color: '#f9a825', group: 'fabric'},
	{label: 'Reporting\nLakehouse', x: 1100, y: 170, w: 190, h: 80, color: '#283593', group: 'fabric'},
	{label: 'Eventhouse\nKQL DB', x: 1370, y: 50, w: 200, h: 80, color: '#00838f', group: 'fabric'},
	{label: 'Eventstream', x: 1370, y: 170, w: 200, h: 80, color: '#00838f', group: 'fabric'},
	{label: 'KQL Shortcuts\n(6 Silver Tables)', x: 1370, y: 290, w: 200, h: 80, color: '#388e3c', group: 'fabric'},
	{label: 'Patient 360\nAgent', x: 880, y: 310, w: 180, h: 75, color: '#4caf50', group: 'fabric'},
	{label: 'Clinical Triage\nAgent', x: 1080, y: 310, w: 180, h: 75, color: '#4caf50', group: 'fabric'},
	{label: 'Cohorting\nAgent', x: 1280, y: 440, w: 180, h: 75, color: '#4caf50', group: 'fabric'},
	{label: 'Real-Time\nDashboard', x: 1480, y: 440, w: 180, h: 75, color: '#e03f8f', group: 'fabric'},
	{label: 'Power BI\nDirect Lake', x: 1080, y: 440, w: 180, h: 75, color: '#e03f8f', group: 'fabric'},
	{label: 'OHIF\nDICOM Viewer', x: 880, y: 440, w: 180, h: 75, color: '#0078d4', group: 'viewer'},
];

const arrows: Array<{from: number; to: number}> = [
	{from: 0, to: 1}, {from: 1, to: 2}, {from: 4, to: 3},
	{from: 5, to: 2}, {from: 2, to: 6}, {from: 6, to: 7},
	{from: 7, to: 8}, {from: 3, to: 10}, {from: 10, to: 9},
	{from: 7, to: 11}, {from: 7, to: 12}, {from: 9, to: 13},
];

export const ArchitectureSlide: React.FC = () => {
	const frame = useCurrentFrame();
	const {fps} = useVideoConfig();

	const titleOpacity = interpolate(frame, [0, 15], [0, 1], {
		extrapolateRight: 'clamp',
	});

	return (
		<AbsoluteFill
			style={{
				background: 'linear-gradient(160deg, #0a0a2e 0%, #12123a 100%)',
				padding: '30px 40px',
			}}
		>
			<h2
				style={{
					fontSize: 48,
					fontWeight: 700,
					color: 'white',
					opacity: titleOpacity,
					fontFamily: 'system-ui, sans-serif',
					margin: 0,
					marginBottom: 10,
				}}
			>
				End-to-End Architecture
			</h2>

			{/* Azure group */}
			<div
				style={{
					position: 'absolute',
					left: 25,
					top: 90,
					width: 810,
					height: 520,
					border: '2px solid rgba(0,120,212,0.4)',
					borderRadius: 12,
					opacity: interpolate(frame, [5, 20], [0, 1], {extrapolateRight: 'clamp'}),
				}}
			>
				<span
					style={{
						position: 'absolute',
						top: -12,
						left: 20,
						background: '#0e0e2a',
						padding: '0 10px',
						color: '#0078d4',
						fontSize: 16,
						fontFamily: 'system-ui, sans-serif',
						fontWeight: 600,
					}}
				>
					Azure Resource Group
				</span>
			</div>

			{/* Fabric group */}
			<div
				style={{
					position: 'absolute',
					left: 855,
					top: 90,
					width: 830,
					height: 520,
					border: '2px solid rgba(128,0,212,0.4)',
					borderRadius: 12,
					opacity: interpolate(frame, [5, 20], [0, 1], {extrapolateRight: 'clamp'}),
				}}
			>
				<span
					style={{
						position: 'absolute',
						top: -12,
						left: 20,
						background: '#0e0e2a',
						padding: '0 10px',
						color: '#8000d4',
						fontSize: 16,
						fontFamily: 'system-ui, sans-serif',
						fontWeight: 600,
					}}
				>
					Microsoft Fabric Workspace
				</span>
			</div>

			{/* Boxes */}
			{boxes.map((box, i) => {
				const delay = 10 + i * 8;
				const progress = spring({
					fps,
					frame: frame - delay,
					config: {damping: 80},
				});
				const opacity = interpolate(progress, [0, 1], [0, 1]);
				const scale = interpolate(progress, [0, 1], [0.8, 1]);

				return (
					<div
						key={i}
						style={{
							position: 'absolute',
							left: box.x,
							top: box.y + 100,
							width: box.w,
							height: box.h,
							borderRadius: 12,
							background: `${box.color}22`,
							border: `2px solid ${box.color}`,
							display: 'flex',
							alignItems: 'center',
							justifyContent: 'center',
							opacity,
							transform: `scale(${scale})`,
						}}
					>
						<span
							style={{
								color: 'white',
							fontSize: 16,
								fontFamily: 'system-ui, sans-serif',
								fontWeight: 600,
								textAlign: 'center',
								whiteSpace: 'pre-line',
								lineHeight: 1.3,
							}}
						>
							{box.label}
						</span>
					</div>
				);
			})}

			{/* Deploy badge */}
			<div
				style={{
					position: 'absolute',
					bottom: 30,
					right: 40,
					opacity: interpolate(frame, [180, 210], [0, 1], {
						extrapolateRight: 'clamp',
					}),
					padding: '12px 24px',
					borderRadius: 8,
					background: 'rgba(0,200,180,0.15)',
					border: '1px solid rgba(0,200,180,0.4)',
					color: '#00c4b4',
					fontSize: 20,
					fontFamily: 'monospace',
				}}
			>
				.\Deploy-All.ps1 — deploys in &lt; 2 hours
			</div>
		</AbsoluteFill>
	);
};
