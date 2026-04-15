/**
 * Animated starfield/firefly background.
 * Renders subtle, slowly drifting particles in Fabric's teal palette.
 * Uses CSS animations (no canvas/requestAnimationFrame) for performance.
 */

import { makeStyles } from "@fluentui/react-components";
import { useMemo } from "react";
import { useReducedMotion } from "../hooks/useReducedMotion";

const PARTICLE_COUNT = 40;

const useStyles = makeStyles({
  container: {
    position: "fixed" as const,
    top: 0,
    left: 0,
    width: "100%",
    height: "100%",
    pointerEvents: "none",
    zIndex: 0,
    overflow: "hidden",
  },
});

// Seeded random for consistent particle positions across renders
function seededRandom(seed: number): number {
  const x = Math.sin(seed * 9301 + 49297) * 49297;
  return x - Math.floor(x);
}

interface Particle {
  id: number;
  x: number;
  y: number;
  size: number;
  opacity: number;
  duration: number;
  delay: number;
  drift: number;
  color: string;
}

function generateParticles(): Particle[] {
  const colors = [
    "rgba(17, 120, 101, 0.3)",  // Fabric teal
    "rgba(42, 172, 148, 0.25)", // Lighter teal
    "rgba(96, 233, 208, 0.2)",  // Cyan
    "rgba(171, 232, 142, 0.15)", // Green accent
    "rgba(106, 214, 249, 0.15)", // Blue accent
  ];

  return Array.from({ length: PARTICLE_COUNT }, (_, i) => {
    const r = (s: number) => seededRandom(i * 100 + s);
    return {
      id: i,
      x: r(1) * 100,
      y: r(2) * 100,
      size: 2 + r(3) * 4,
      opacity: 0.1 + r(4) * 0.3,
      duration: 12 + r(5) * 24, // 12-36s cycle (25% faster)
      delay: r(6) * -24, // staggered start
      drift: 6.25 + r(7) * 18.75, // drift distance in vh (25% more)
      color: colors[Math.floor(r(8) * colors.length)],
    };
  });
}

export function AnimatedBackground() {
  const styles = useStyles();
  const reducedMotion = useReducedMotion();
  const particles = useMemo(generateParticles, []);

  if (reducedMotion) {
    return null;
  }

  return (
    <div className={styles.container}>
      <style>{`
        @keyframes firefly-float {
          0%, 100% {
            transform: translate(0, 0) scale(1);
            opacity: var(--p-opacity);
          }
          25% {
            transform: translate(calc(var(--p-drift) * 0.5), calc(var(--p-drift) * -0.3)) scale(1.2);
            opacity: calc(var(--p-opacity) * 1.5);
          }
          50% {
            transform: translate(var(--p-drift), calc(var(--p-drift) * 0.2)) scale(0.8);
            opacity: calc(var(--p-opacity) * 0.5);
          }
          75% {
            transform: translate(calc(var(--p-drift) * -0.3), var(--p-drift)) scale(1.1);
            opacity: var(--p-opacity);
          }
        }
        @keyframes firefly-pulse {
          0%, 100% { box-shadow: 0 0 2px currentColor; }
          50% { box-shadow: 0 0 8px currentColor, 0 0 16px currentColor; }
        }
        @media (prefers-reduced-motion: reduce) {
          .firefly-particle {
            animation: none !important;
            opacity: 0.15 !important;
          }
        }
      `}</style>
      {particles.map((p) => (
        <div
          key={p.id}
          className="firefly-particle"
          style={{
            position: "absolute",
            left: `${p.x}%`,
            top: `${p.y}%`,
            width: `${p.size}px`,
            height: `${p.size}px`,
            borderRadius: "50%",
            backgroundColor: p.color,
            color: p.color,
            opacity: p.opacity,
            animation: `firefly-float ${p.duration}s ease-in-out ${p.delay}s infinite, firefly-pulse ${p.duration * 0.7}s ease-in-out ${p.delay}s infinite`,
            ["--p-opacity" as string]: p.opacity,
            ["--p-drift" as string]: `${p.drift}vh`,
            willChange: "transform, opacity",
          }}
        />
      ))}
    </div>
  );
}
