import React from 'react';
import {
	AbsoluteFill,
	interpolate,
	useCurrentFrame,
	useVideoConfig,
} from 'remotion';
import {TransitionSeries, linearTiming} from '@remotion/transitions';
import {fade} from '@remotion/transitions/fade';
import {TitleSlide} from './slides/TitleSlide';
import {BulletSlide} from './slides/BulletSlide';
import {ArchitectureSlide} from './slides/ArchitectureSlide';
import {PhaseSlide} from './slides/PhaseSlide';
import {DataFlowSlide} from './slides/DataFlowSlide';
import {AgentsSlide} from './slides/AgentsSlide';
import {ClosingSlide} from './slides/ClosingSlide';

const TRANSITION = 20;

export const ReadmeVideo: React.FC = () => {
	return (
		<AbsoluteFill style={{backgroundColor: '#0a0a1a'}}>
			<TransitionSeries>
				{/* 1. Title */}
				<TransitionSeries.Sequence durationInFrames={630}>
					<TitleSlide />
				</TransitionSeries.Sequence>
				<TransitionSeries.Transition
					timing={linearTiming({durationInFrames: TRANSITION})}
					presentation={fade()}
				/>

				{/* 2. What it demonstrates */}
				<TransitionSeries.Sequence durationInFrames={690}>
					<BulletSlide
						title="What This Solution Demonstrates"
						bullets={[
							{icon: '⚡', text: 'Real-Time Intelligence — Masimo telemetry → Eventstream → Eventhouse with KQL clinical alerts'},
							{icon: '🏥', text: 'Healthcare Data Solutions — 10K FHIR R4 patients flow into Silver Lakehouse via HDS'},
							{icon: '🩻', text: 'DICOM Medical Imaging — TCIA chest CTs re-tagged with Synthea patient IDs'},
							{icon: '🤖', text: 'Data Agents — Patient 360 + Clinical Triage federate KQL + Lakehouse data'},
							{icon: '📊', text: 'Power BI Direct Lake + OHIF DICOM Viewer + Cohorting Agent'},
							{icon: '💎', text: 'OneLake — One copy, queryable from KQL, Spark, SQL, and Power BI'},
						]}
					/>
				</TransitionSeries.Sequence>
				<TransitionSeries.Transition
					timing={linearTiming({durationInFrames: TRANSITION})}
					presentation={fade()}
				/>

				{/* 3. Architecture Overview */}
				<TransitionSeries.Sequence durationInFrames={750}>
					<ArchitectureSlide />
				</TransitionSeries.Sequence>
				<TransitionSeries.Transition
					timing={linearTiming({durationInFrames: TRANSITION})}
					presentation={fade()}
				/>

				{/* 4. Phase 1 */}
				<TransitionSeries.Sequence durationInFrames={690}>
					<PhaseSlide
						phase={1}
						title="Infrastructure & Ingestion"
						color="#ff8c00"
						duration="~25 min"
						items={[
							'Event Hub + ACR + Emulator ACI',
							'Fabric Workspace + Capacity + Identity',
							'Synthea → 10K patients → FHIR Service',
							'DICOM Loader → TCIA → ADLS Gen2',
							'Eventhouse + Eventstream + KQL + Dashboard',
							'FHIR $export to ADLS Gen2',
						]}
					/>
				</TransitionSeries.Sequence>
				<TransitionSeries.Transition
					timing={linearTiming({durationInFrames: TRANSITION})}
					presentation={fade()}
				/>

				{/* 5. Phase 2 */}
				<TransitionSeries.Sequence durationInFrames={690}>
					<PhaseSlide
						phase={2}
						title="HDS Enrichment & Data Agents"
						color="#00a000"
						duration="~35 min"
						items={[
							'Bronze FHIR-HDS + DICOM-HDS shortcuts',
							'Clinical + Imaging + OMOP pipelines',
							'6 KQL Silver Lakehouse shortcuts',
							'Enriched fn_ClinicalAlerts + fn_AlertLocationMap',
							'Clinical Alerts Map Dashboard',
							'Patient 360 + Clinical Triage Data Agents',
						]}
					/>
				</TransitionSeries.Sequence>
				<TransitionSeries.Transition
					timing={linearTiming({durationInFrames: TRANSITION})}
					presentation={fade()}
				/>

				{/* 6. Phase 3 */}
				<TransitionSeries.Sequence durationInFrames={660}>
					<PhaseSlide
						phase={3}
						title="Imaging & Cohorting"
						color="#d40000"
						duration="~4 min"
						items={[
							'Cohorting Data Agent (OMOP CDM queries)',
							'OHIF DICOM Viewer (Static Web App + DICOMweb Proxy)',
							'Materialization Notebook + Reporting Lakehouse',
							'Power BI Imaging Report (Direct Lake)',
						]}
					/>
				</TransitionSeries.Sequence>
				<TransitionSeries.Transition
					timing={linearTiming({durationInFrames: TRANSITION})}
					presentation={fade()}
				/>

				{/* 7. Data Agents */}
				<TransitionSeries.Sequence durationInFrames={690}>
					<AgentsSlide />
				</TransitionSeries.Sequence>
				<TransitionSeries.Transition
					timing={linearTiming({durationInFrames: TRANSITION})}
					presentation={fade()}
				/>

				{/* 8. Closing */}
				<TransitionSeries.Sequence durationInFrames={660}>
					<ClosingSlide />
				</TransitionSeries.Sequence>
			</TransitionSeries>
		</AbsoluteFill>
	);
};
