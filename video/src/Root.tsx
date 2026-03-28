import React from 'react';
import {Composition} from 'remotion';
import {ReadmeVideo} from './ReadmeVideo';

export const Root: React.FC = () => {
	return (
		<>
			<Composition
				id="ReadmeOverview"
				component={ReadmeVideo}
				durationInFrames={5320}
				width={1920}
				height={1080}
				fps={30}
				defaultProps={{}}
			/>
		</>
	);
};
