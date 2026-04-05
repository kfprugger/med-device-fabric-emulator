'use client';
import * as React from 'react';
export const useSkeletonContextValues = (state)=>{
    const { animation, appearance, size, shape } = state;
    const skeletonGroup = React.useMemo(()=>({
            animation,
            appearance,
            size,
            shape
        }), [
        animation,
        appearance,
        size,
        shape
    ]);
    return {
        skeletonGroup
    };
};
